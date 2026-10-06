//! The shared cross-replica journal poll (LQ4). Every LiveQuery stream needs the
//! durable CDC journal as its delta source on replicas that do not hold the CDC
//! tailer lease, and each stream used to scan the journal on its own every
//! 500 ms — N subscriptions to one tenant's entity meant N identical scans. One
//! poller per (topic, tenant, project) now scans once per tick and broadcasts the
//! batch to every subscription of that scope.
//!
//! The scan is still the tenant/project-scoped one
//! ([`crate::cdc::CdcEngine::try_journal_scan_after`]), so a feed never carries
//! another tenant's rows, and each subscriber still re-checks scope and applies
//! its own predicate in the forwarder. A batch carries the journal watermarks it
//! covers (`from` exclusive, `to` inclusive): a subscriber whose own watermark is
//! behind `from` (it lagged, or resumed from an older cursor) catches up with a
//! private scan before applying the shared batches again, so a shared feed never
//! opens a gap.
//!
//! A poller exits on the first tick that finds no subscriber left, and on a
//! journal read failure (after telling its subscribers, which close their
//! streams with a retryable error exactly as before).

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio::sync::broadcast;
use tokio::time::{Instant, MissedTickBehavior, interval_at};

use crate::cdc::{CdcEngine, CdcEnvelope};

/// A position in the journal's canonical `(published_at, event_id)` order.
pub(crate) type Watermark = (DateTime<Utc>, String);

/// How often a shared poller scans the journal.
pub(crate) const JOURNAL_TAIL_POLL: Duration = Duration::from_millis(500);

/// Batches a slow subscriber may fall behind before it lags; a lagged
/// subscriber catches up from the journal itself, so this bounds memory, not
/// correctness.
const FEED_CAPACITY: usize = 64;

/// Scans per tick while the journal keeps returning rows, so a burst drains in
/// one tick without letting one poller monopolise the pool.
const MAX_SCANS_PER_TICK: usize = 16;

/// One poll's result for a scope.
#[derive(Debug)]
pub(crate) struct FeedBatch {
    /// Watermark the scan started strictly after.
    pub(crate) from: Watermark,
    /// Watermark of the last row the scan covered (in scope or not).
    pub(crate) to: Watermark,
    /// The in-scope events in journal order.
    pub(crate) events: Vec<CdcEnvelope>,
    /// Set when the journal could not be read; the feed ends after it.
    pub(crate) error: Option<String>,
}

impl FeedBatch {
    fn failed(error: String) -> Self {
        let epoch = epoch_watermark();
        Self {
            from: epoch.clone(),
            to: epoch,
            events: Vec::new(),
            error: Some(error),
        }
    }
}

/// The journal's lower bound: every real row sorts after it.
pub(crate) fn epoch_watermark() -> Watermark {
    (
        DateTime::<Utc>::from_timestamp(0, 0).expect("unix epoch is a valid timestamp"),
        String::new(),
    )
}

/// An envelope's journal position.
pub(crate) fn envelope_watermark(envelope: &CdcEnvelope) -> Watermark {
    (envelope.published_at, envelope.event_id.clone())
}

type FeedKey = (String, String, String);
type FeedSender = broadcast::Sender<Arc<FeedBatch>>;

fn registry() -> &'static Mutex<HashMap<FeedKey, FeedSender>> {
    static FEEDS: OnceLock<Mutex<HashMap<FeedKey, FeedSender>>> = OnceLock::new();
    FEEDS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Number of live shared pollers (for tests and diagnostics).
#[cfg(test)]
pub(crate) fn active_feed_count() -> usize {
    registry().lock().unwrap_or_else(|p| p.into_inner()).len()
}

/// Join the shared feed for `(topic, tenant, project)`, starting its poller if
/// none runs. Join BEFORE reading the subscriber's own journal head: every batch
/// produced after this call reaches the returned receiver, so the head the
/// subscriber reads next can never sit past the feed's first batch unseen.
pub(crate) fn join(
    cdc: &Arc<CdcEngine>,
    topic: &str,
    tenant_id: &str,
    project_id: &str,
    batch: i64,
) -> broadcast::Receiver<Arc<FeedBatch>> {
    let key: FeedKey = (
        topic.to_string(),
        tenant_id.to_string(),
        project_id.to_string(),
    );
    let mut feeds = registry().lock().unwrap_or_else(|p| p.into_inner());
    if let Some(sender) = feeds.get(&key) {
        return sender.subscribe();
    }
    let (sender, receiver) = broadcast::channel(FEED_CAPACITY);
    feeds.insert(key.clone(), sender.clone());
    drop(feeds);
    tokio::spawn(run_feed(cdc.clone(), key, sender, batch));
    receiver
}

/// Remove this poller's registry entry. With `only_if_idle`, only when no
/// subscriber is left; the check and the removal share the registry lock with
/// [`join`], so a subscriber cannot attach to a poller that is exiting.
fn retire(key: &FeedKey, sender: &FeedSender, only_if_idle: bool) -> bool {
    let mut feeds = registry().lock().unwrap_or_else(|p| p.into_inner());
    if only_if_idle && sender.receiver_count() > 0 {
        return false;
    }
    if feeds
        .get(key)
        .is_some_and(|current| current.same_channel(sender))
    {
        feeds.remove(key);
    }
    true
}

async fn run_feed(cdc: Arc<CdcEngine>, key: FeedKey, sender: FeedSender, batch: i64) {
    let (topic, tenant_id, project_id) = (&key.0, &key.1, &key.2);
    let mut watermark = match cdc.journal_head_watermark(topic).await {
        Ok(head) => head,
        Err(err) => {
            fail(&key, &sender, topic, err);
            return;
        }
    };
    let mut ticker = interval_at(Instant::now() + JOURNAL_TAIL_POLL, JOURNAL_TAIL_POLL);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        if retire(&key, &sender, true) {
            return;
        }
        for _ in 0..MAX_SCANS_PER_TICK {
            let scan = cdc
                .try_journal_scan_after(
                    topic,
                    tenant_id,
                    project_id,
                    watermark.0,
                    watermark.1.clone(),
                    batch,
                )
                .await;
            match scan {
                Ok((events, Some(last))) => {
                    let from = std::mem::replace(&mut watermark, last);
                    let _ = sender.send(Arc::new(FeedBatch {
                        from,
                        to: watermark.clone(),
                        events,
                        error: None,
                    }));
                }
                Ok((_, None)) => break,
                Err(err) => {
                    fail(&key, &sender, topic, err);
                    return;
                }
            }
        }
    }
}

fn fail(key: &FeedKey, sender: &FeedSender, topic: &str, err: String) {
    tracing::warn!(
        topic = %topic,
        error = %err,
        "live query journal feed stopped: the CDC journal cannot be read"
    );
    let _ = sender.send(Arc::new(FeedBatch::failed(err)));
    retire(key, sender, false);
}

#[cfg(test)]
mod shared_tail_tests {
    use super::{envelope_watermark, epoch_watermark};

    /// Watermarks order like the journal: by publish time, then event id; the
    /// epoch sorts before every real row.
    #[test]
    fn watermarks_order_like_the_journal() {
        let at = |secs: i64| chrono::DateTime::<chrono::Utc>::from_timestamp(secs, 0).unwrap();
        let envelope = |secs: i64, id: &str| crate::cdc::CdcEnvelope {
            event_id: id.to_string(),
            topic: "udb.t".to_string(),
            partition_key: String::new(),
            payload_json: "{}".to_string(),
            published_at: at(secs),
        };
        let a = envelope_watermark(&envelope(10, "0000-a"));
        let b = envelope_watermark(&envelope(10, "0000-b"));
        let c = envelope_watermark(&envelope(11, "0000-0"));
        assert!(epoch_watermark() < a);
        assert!(a < b);
        assert!(b < c);
    }
}
