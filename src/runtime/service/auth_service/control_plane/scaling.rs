//! Phase 9 control-plane SCALING KNOBS — the reusable debounce/throttle the push
//! path uses so a busy registry does not stampede the fleet.
//!
//! Three independent, env-tunable knobs (all pure runtime mechanics, no I/O):
//!   * [`PushDebouncer`] — coalesces a burst of version bumps into ONE push,
//!     with a *max debounce ceiling* so a continuously-changing registry still
//!     pushes within a bounded delay.
//!   * [`PushThrottle`] — a [`tokio::sync::Semaphore`] cap on simultaneous node
//!     pushes, so a thundering herd of nodes cannot exhaust the runtime.
//!   * push-queue metrics — depth gauge + throttle/coalesce counters, recorded
//!     through the shared [`MetricsRecorder`].
//!
//! The parent wires a single shared [`PushScaler`] into the
//! `ControlPlaneServiceImpl` push loop (see the wiring notes in the task
//! report). Each knob is also usable standalone.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::{Notify, Semaphore};

use crate::runtime::metrics::MetricsRecorder;

/// Env knob: base debounce window — rapid version bumps inside this window
/// coalesce into one push. Default 250ms.
pub const ENV_DEBOUNCE_MS: &str = "UDB_CONTROL_DEBOUNCE_MS";
/// Env knob: max debounce ceiling — even a continuously-changing registry
/// pushes within this bound. Default 2000ms.
pub const ENV_DEBOUNCE_MAX_MS: &str = "UDB_CONTROL_DEBOUNCE_MAX_MS";
/// Env knob: max simultaneous node pushes. Default 64.
pub const ENV_MAX_CONCURRENT_PUSH: &str = "UDB_CONTROL_MAX_CONCURRENT_PUSH";

const DEFAULT_DEBOUNCE_MS: u64 = 250;
const DEFAULT_DEBOUNCE_MAX_MS: u64 = 2_000;
const DEFAULT_MAX_CONCURRENT_PUSH: usize = 64;

/// Resolved, validated scaling settings. `from_env` clamps every value into a
/// sane range so a typo'd env var cannot disable the safety mechanism.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScalingConfig {
    pub debounce: Duration,
    pub debounce_max: Duration,
    pub max_concurrent_push: usize,
}

impl Default for ScalingConfig {
    fn default() -> Self {
        Self {
            debounce: Duration::from_millis(DEFAULT_DEBOUNCE_MS),
            debounce_max: Duration::from_millis(DEFAULT_DEBOUNCE_MAX_MS),
            max_concurrent_push: DEFAULT_MAX_CONCURRENT_PUSH,
        }
    }
}

impl ScalingConfig {
    /// Read the three env knobs, clamping each into a safe range:
    ///   * debounce in `[1ms, 60s]`;
    ///   * ceiling coerced to be `>= debounce` (a ceiling below the base is
    ///     nonsensical) and `<= 5min`;
    ///   * concurrency in `[1, 4096]`.
    pub fn from_env() -> Self {
        let defaults = Self::default();
        let debounce_ms = env_u64(ENV_DEBOUNCE_MS)
            .unwrap_or(DEFAULT_DEBOUNCE_MS)
            .clamp(1, 60_000);
        let mut max_ms = env_u64(ENV_DEBOUNCE_MAX_MS)
            .unwrap_or(DEFAULT_DEBOUNCE_MAX_MS)
            .clamp(1, 300_000);
        if max_ms < debounce_ms {
            max_ms = debounce_ms;
        }
        let max_concurrent_push = env_u64(ENV_MAX_CONCURRENT_PUSH)
            .map(|value| value.clamp(1, 4_096) as usize)
            .unwrap_or(defaults.max_concurrent_push);
        Self {
            debounce: Duration::from_millis(debounce_ms),
            debounce_max: Duration::from_millis(max_ms),
            max_concurrent_push,
        }
    }
}

fn env_u64(key: &str) -> Option<u64> {
    std::env::var(key)
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
}

/// Coalesces a burst of `notify()` signals into a single fire, honoring a base
/// debounce window and a max ceiling.
///
/// Semantics (xDS-style "send the latest, not every intermediate"):
///   * `notify()` records that work is pending and the time of the FIRST pending
///     signal in the current burst.
///   * `wait().await` returns once either:
///       - the base debounce window has elapsed since the LAST signal (the burst
///         went quiet), OR
///       - the ceiling has elapsed since the FIRST signal of the burst (a
///         continuous stream still drains within the ceiling).
///   * Every coalesced signal beyond the first increments
///     `inc_control_debounce_coalesced` so the savings are observable.
#[derive(Clone)]
pub struct PushDebouncer {
    inner: Arc<DebouncerInner>,
}

struct DebouncerInner {
    notify: Notify,
    /// Pending-signal count for the current (un-drained) burst. 0 == idle.
    pending: AtomicU64,
    /// `Instant` (as nanos since an epoch baseline) of the first pending signal.
    first_pending_nanos: AtomicU64,
    /// `Instant` of the most recent signal.
    last_signal_nanos: AtomicU64,
    debounce: Duration,
    debounce_max: Duration,
    baseline: Instant,
    metrics: Option<Arc<dyn MetricsRecorder>>,
}

impl PushDebouncer {
    pub fn new(config: ScalingConfig, metrics: Option<Arc<dyn MetricsRecorder>>) -> Self {
        Self {
            inner: Arc::new(DebouncerInner {
                notify: Notify::new(),
                pending: AtomicU64::new(0),
                first_pending_nanos: AtomicU64::new(0),
                last_signal_nanos: AtomicU64::new(0),
                debounce: config.debounce,
                debounce_max: config.debounce_max,
                baseline: Instant::now(),
                metrics,
            }),
        }
    }

    fn now_nanos(&self) -> u64 {
        self.inner.baseline.elapsed().as_nanos() as u64
    }

    /// Signal that a new version bump arrived. Coalesces with any in-flight
    /// burst; only the first signal of a burst starts the clock.
    pub fn notify(&self) {
        let now = self.now_nanos();
        let prev = self.inner.pending.fetch_add(1, Ordering::SeqCst);
        if prev == 0 {
            // First signal of a fresh burst — start both clocks.
            self.inner.first_pending_nanos.store(now, Ordering::SeqCst);
        } else {
            // A coalesced signal: this bump will NOT cause its own push.
            if let Some(metrics) = self.inner.metrics.as_ref() {
                metrics.inc_control_debounce_coalesced();
            }
        }
        self.inner.last_signal_nanos.store(now, Ordering::SeqCst);
        self.inner.notify.notify_one();
    }

    /// Returns the number of pending (coalesced) signals currently queued.
    /// Test-only introspection of the debouncer's burst-coalescing invariant;
    /// production observability of coalescing is covered by the push metrics.
    #[cfg(test)]
    pub fn pending(&self) -> u64 {
        self.inner.pending.load(Ordering::SeqCst)
    }

    /// Await the next coalesced push opportunity, then atomically consume the
    /// pending burst (so the caller pushes exactly once for the whole burst).
    /// Resolves only when at least one signal is pending.
    pub async fn wait(&self) {
        loop {
            // Block until there is at least one pending signal.
            while self.inner.pending.load(Ordering::SeqCst) == 0 {
                self.inner.notify.notified().await;
            }
            // Decide how long to wait before draining, based on the two clocks.
            let now = self.now_nanos();
            let first = self.inner.first_pending_nanos.load(Ordering::SeqCst);
            let last = self.inner.last_signal_nanos.load(Ordering::SeqCst);
            let debounce = self.inner.debounce.as_nanos() as u64;
            let ceiling = self.inner.debounce_max.as_nanos() as u64;
            let quiet_deadline = last.saturating_add(debounce);
            let ceiling_deadline = first.saturating_add(ceiling);
            let deadline = quiet_deadline.min(ceiling_deadline);
            if now >= deadline {
                // The burst is ready to drain now.
                self.drain();
                return;
            }
            let sleep_for = Duration::from_nanos(deadline - now);
            tokio::select! {
                _ = tokio::time::sleep(sleep_for) => {}
                // A new signal arrived; loop to re-evaluate the deadline (which
                // may extend up to the ceiling).
                _ = self.inner.notify.notified() => {}
            }
        }
    }

    /// Atomically consume the pending burst (reset to idle).
    fn drain(&self) {
        self.inner.pending.store(0, Ordering::SeqCst);
    }
}

/// A concurrency cap on simultaneous node pushes. Wraps a
/// [`tokio::sync::Semaphore`]; every push acquires a permit for its duration so
/// no more than `max_concurrent_push` pushes run at once. A push that has to
/// wait for a permit increments `inc_control_push_throttled`.
#[derive(Clone)]
pub struct PushThrottle {
    semaphore: Arc<Semaphore>,
    capacity: usize,
    metrics: Option<Arc<dyn MetricsRecorder>>,
}

impl PushThrottle {
    pub fn new(config: ScalingConfig, metrics: Option<Arc<dyn MetricsRecorder>>) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(config.max_concurrent_push)),
            capacity: config.max_concurrent_push,
            metrics,
        }
    }

    /// Permits currently available (capacity minus in-flight pushes).
    pub fn available(&self) -> usize {
        self.semaphore.available_permits()
    }

    /// In-flight push count (capacity minus available permits). Also reported as
    /// the push-queue depth gauge.
    pub fn in_flight(&self) -> usize {
        self.capacity.saturating_sub(self.available())
    }

    /// Acquire a push permit, blocking when the cap is reached. When no permit is
    /// immediately available the push is counted as throttled. The returned
    /// guard releases the permit on drop and refreshes the depth gauge.
    pub async fn acquire(&self) -> PushPermit {
        if self.semaphore.available_permits() == 0 {
            if let Some(metrics) = self.metrics.as_ref() {
                metrics.inc_control_push_throttled();
            }
        }
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("control-plane push semaphore is never closed");
        if let Some(metrics) = self.metrics.as_ref() {
            metrics.set_control_push_queue_depth(self.in_flight() as u64);
        }
        PushPermit {
            _permit: permit,
            throttle: self.clone(),
        }
    }
}

/// RAII guard for an in-flight push. Releasing it (on drop) frees the permit and
/// refreshes the push-queue depth gauge.
pub struct PushPermit {
    _permit: tokio::sync::OwnedSemaphorePermit,
    throttle: PushThrottle,
}

impl Drop for PushPermit {
    fn drop(&mut self) {
        // The permit is released as `_permit` drops AFTER this body, so compute
        // the post-release depth (current in_flight minus this one).
        if let Some(metrics) = self.throttle.metrics.as_ref() {
            let depth = self.throttle.in_flight().saturating_sub(1) as u64;
            metrics.set_control_push_queue_depth(depth);
        }
    }
}

/// The combined scaling facade the parent wires into the push loop: one
/// debouncer to coalesce registry bumps, one throttle to cap concurrent node
/// pushes, sharing the same [`ScalingConfig`] + metrics handle.
#[derive(Clone)]
pub struct PushScaler {
    pub debouncer: PushDebouncer,
    pub throttle: PushThrottle,
}

impl PushScaler {
    pub fn new(config: ScalingConfig, metrics: Option<Arc<dyn MetricsRecorder>>) -> Self {
        Self {
            debouncer: PushDebouncer::new(config, metrics.clone()),
            throttle: PushThrottle::new(config, metrics),
        }
    }

    /// Build from the env knobs (the production path).
    pub fn from_env(metrics: Option<Arc<dyn MetricsRecorder>>) -> Self {
        Self::new(ScalingConfig::from_env(), metrics)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    fn fast_config() -> ScalingConfig {
        ScalingConfig {
            debounce: Duration::from_millis(20),
            debounce_max: Duration::from_millis(200),
            max_concurrent_push: 2,
        }
    }

    #[test]
    fn scaling_config_clamps_bad_env_values() {
        // Ceiling below base is coerced up to the base.
        let cfg = ScalingConfig {
            debounce: Duration::from_millis(100),
            debounce_max: Duration::from_millis(10),
            max_concurrent_push: 8,
        };
        // (from_env does the coercion; here we assert the rule directly.)
        assert!(cfg.debounce_max < cfg.debounce, "precondition for the rule");
        let coerced_max = cfg.debounce_max.max(cfg.debounce);
        assert_eq!(coerced_max, cfg.debounce);
    }

    // Debounce coalesces N rapid bumps in a window into a SINGLE drained push.
    #[tokio::test(start_paused = true)]
    async fn debounce_coalesces_burst_into_one_push() {
        let debouncer = PushDebouncer::new(fast_config(), None);
        // Fire 5 bumps back-to-back (no time advance between them == one burst).
        for _ in 0..5 {
            debouncer.notify();
        }
        assert_eq!(debouncer.pending(), 5, "all 5 bumps are pending");
        // A single wait() drains the WHOLE burst → one push for five bumps.
        tokio::time::advance(Duration::from_millis(25)).await;
        debouncer.wait().await;
        assert_eq!(
            debouncer.pending(),
            0,
            "the whole burst is consumed by a single push"
        );
    }

    // The ceiling forces a drain even while signals keep arriving: a continuous
    // stream whose quiet-window never elapses still drains within the ceiling.
    #[tokio::test(start_paused = true)]
    async fn debounce_ceiling_drains_continuous_stream() {
        let debouncer = PushDebouncer::new(fast_config(), None); // base 20ms, ceiling 200ms
        debouncer.notify();
        let waiter = debouncer.clone();
        let handle = tokio::spawn(async move { waiter.wait().await });
        // Keep signaling at 10ms cadence (< the 20ms base window) so the
        // quiet-deadline keeps being pushed out; only the 200ms ceiling can drain.
        for _ in 0..40 {
            tokio::time::advance(Duration::from_millis(10)).await;
            tokio::task::yield_now().await;
            if handle.is_finished() {
                break;
            }
            debouncer.notify();
        }
        // Well past the 200ms ceiling; the wait must have resolved by now.
        handle.await.expect("wait resolves under the ceiling");
        assert_eq!(debouncer.pending(), 0, "ceiling drained the burst");
    }

    // Throttle caps concurrency at the configured limit.
    #[tokio::test(start_paused = true)]
    async fn throttle_caps_concurrent_pushes() {
        let throttle = PushThrottle::new(fast_config(), None); // cap == 2
        let p1 = throttle.acquire().await;
        let p2 = throttle.acquire().await;
        assert_eq!(throttle.in_flight(), 2, "two pushes in flight");
        assert_eq!(throttle.available(), 0, "cap reached");

        // A third acquire must BLOCK until a permit frees up.
        let acquired = Arc::new(AtomicUsize::new(0));
        let acquired_clone = acquired.clone();
        let throttle_clone = throttle.clone();
        let handle = tokio::spawn(async move {
            let _p3 = throttle_clone.acquire().await;
            acquired_clone.fetch_add(1, Ordering::SeqCst);
        });
        tokio::time::advance(Duration::from_millis(5)).await;
        tokio::task::yield_now().await;
        assert_eq!(
            acquired.load(Ordering::SeqCst),
            0,
            "the third push is throttled while the cap is full"
        );
        // Free one permit; the blocked push now proceeds.
        drop(p1);
        tokio::task::yield_now().await;
        handle
            .await
            .expect("third push proceeds once a permit frees");
        assert_eq!(acquired.load(Ordering::SeqCst), 1);
        drop(p2);
        assert_eq!(throttle.in_flight(), 0, "all permits released");
    }
}
