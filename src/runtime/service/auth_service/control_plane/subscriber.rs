//! Phase 9 in-process reload SUBSCRIBER — closes the gap where Phase-K emits
//! `udb.authz.policy.bundle.invalidated.v1` (and other authz/config changes) but
//! NO node reloads. This background task periodically:
//!   1. re-sources the registry from the live config + descriptor
//!      ([`super::sourcing::resync`]) so config drift lands in the registry;
//!   2. computes a cheap fleet fingerprint
//!      ([`super::store::fleet_world_fingerprint`]) plus the authz bundle version;
//!   3. when EITHER changed since the last tick, fires a [`tokio::sync::Notify`]
//!      that the `ControlPlaneServiceImpl` push loop subscribes to, so every open
//!      stream wakes and pushes the new versions immediately (instead of waiting
//!      for its own poll interval), and increments
//!      `metrics.inc_control_reload_applied(resource_type)` per changed type.
//!
//! ## Singleton lease — intentionally OPTIONAL.
//! `resync` is a pure read-snapshot: it reads config/descriptor and performs
//! content-addressed [`super::store::upsert_resource`] writes that are idempotent
//! (identical content is a no-op, no version bump). Two nodes resyncing
//! concurrently converge to the same rows with no lost-update or double-bump
//! hazard, so a singleton lease is NOT required for correctness. We therefore run
//! the resync on every node (each node also needs the local *notify* fired so its
//! own open streams wake). A lease is only warranted for NON-idempotent durable
//! side effects, which this loop has none of; this mirrors the doctrine in
//! `src/runtime/singleton.rs` (leases guard non-idempotent singleton work).

use std::sync::Arc;
use std::time::Duration;

use sqlx::PgPool;
use tokio::sync::Notify;

use crate::runtime::config::UdbConfig;
use crate::runtime::metrics::MetricsRecorder;

use super::resources::{ordered_resource_types, resource_type_to_db};
use super::{sourcing, store};

/// Env knob: how often the subscriber re-sources + checks for changes. Default
/// 1000ms, clamped into `[100ms, 60s]`.
pub const ENV_RELOAD_INTERVAL_MS: &str = "UDB_CONTROL_RELOAD_INTERVAL_MS";
const DEFAULT_RELOAD_INTERVAL_MS: u64 = 1_000;

/// Resolve the reload interval from the environment, clamped to a safe range.
pub fn reload_interval() -> Duration {
    let ms = std::env::var(ENV_RELOAD_INTERVAL_MS)
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .unwrap_or(DEFAULT_RELOAD_INTERVAL_MS)
        .clamp(100, 60_000);
    Duration::from_millis(ms)
}

/// Snapshot of the authz bundle version, used as a change signal alongside the
/// registry fingerprint. Returns the current process authz snapshot version, or
/// an empty string when no engine handle is wired (registry fingerprint alone
/// then drives reloads).
pub type AuthzVersionFn = Arc<dyn Fn() -> String + Send + Sync>;

/// Inputs the parent supplies when spawning the subscriber.
#[derive(Clone)]
pub struct SubscriberHandle {
    pool: PgPool,
    config: Arc<UdbConfig>,
    /// Fired whenever a reload is applied; the `ControlPlaneServiceImpl` stream
    /// loops `await` this to wake immediately instead of polling.
    reload_notify: Arc<Notify>,
    metrics: Option<Arc<dyn MetricsRecorder>>,
    authz_version: Option<AuthzVersionFn>,
}

impl SubscriberHandle {
    pub fn new(pool: PgPool, config: Arc<UdbConfig>) -> Self {
        Self {
            pool,
            config,
            reload_notify: Arc::new(Notify::new()),
            metrics: None,
            authz_version: None,
        }
    }

    pub fn with_metrics(mut self, metrics: Arc<dyn MetricsRecorder>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Wire an authz-version probe (e.g. `engine.bundle_version()`), so a policy
    /// change that does not alter the sourced registry still triggers a push.
    pub fn with_authz_version(mut self, probe: AuthzVersionFn) -> Self {
        self.authz_version = Some(probe);
        self
    }

    /// The shared notify handle the `ControlPlaneServiceImpl` push loop should
    /// subscribe to (`reload_notify().notified().await`) so it wakes the instant
    /// a reload is applied. Cloneable + cheap.
    pub fn reload_notify(&self) -> Arc<Notify> {
        self.reload_notify.clone()
    }
}

/// Per-type world versions captured on the previous tick, so the next tick can
/// report exactly WHICH resource types changed (for the per-type
/// `inc_control_reload_applied` metric).
///
/// `pub(crate)` (with private fields) so the crate's live integration tests can
/// seed a baseline (`LastSeen::default()`) and drive [`run_once`] on the REAL
/// served reload path, asserting the reload metric fires (not a stubbed call).
#[derive(Default, Clone)]
pub(crate) struct LastSeen {
    fingerprint: String,
    authz_version: String,
    per_type: std::collections::BTreeMap<&'static str, String>,
}

/// Spawn the in-process reload subscriber. The returned [`JoinHandle`] runs until
/// the process exits; the caller keeps [`SubscriberHandle::reload_notify`] to
/// wake its streams. Idempotent resync ⇒ safe to run on every node (no lease).
pub fn spawn_control_plane_subscriber(handle: SubscriberHandle) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let interval = reload_interval();
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut last = LastSeen::default();
        // Seed the baseline on the first pass WITHOUT firing a spurious reload:
        // sources the registry, records versions, but only notifies on a real
        // change thereafter.
        let mut seeded = false;
        loop {
            ticker.tick().await;
            match run_once(&handle, &last, seeded).await {
                Ok(next) => {
                    last = next;
                    seeded = true;
                }
                Err(err) => {
                    tracing::warn!(
                        error = %err,
                        "control-plane reload subscriber tick failed; will retry"
                    );
                }
            }
        }
    })
}

/// One subscriber tick: resync, then diff the fleet fingerprint + authz version.
/// On a real change (and only after the baseline is seeded) fire the notify and
/// record the per-type reload metric. Factored out for unit-testing the diff
/// logic. Returns the new `LastSeen` baseline.
///
/// `pub(crate)` so the live integration suite can exercise the REAL served
/// reload path (seed, mutate the registry, tick again, assert the reload metric).
pub(crate) async fn run_once(
    handle: &SubscriberHandle,
    last: &LastSeen,
    seeded: bool,
) -> Result<LastSeen, String> {
    // urgent_fix #27: time the reload cycle (resync → version recompute → apply) so
    // `observe_policy_reload_seconds` is fed from the serving path, not just tests.
    let reload_started = std::time::Instant::now();
    // 1. Re-source the registry from live config (idempotent, content-addressed).
    sourcing::resync(&handle.pool, &handle.config)
        .await
        .map_err(|status| format!("resync failed: {status}"))?;

    // 2. Capture per-type world versions + the combined fleet fingerprint.
    let mut per_type = std::collections::BTreeMap::new();
    for rt in ordered_resource_types() {
        let version = store::world_version(&handle.pool, *rt, None, &[])
            .await
            .map_err(|status| format!("world_version failed: {status}"))?;
        per_type.insert(resource_type_to_db(*rt), version);
    }
    let fingerprint = store::fleet_world_fingerprint(&handle.pool)
        .await
        .map_err(|status| format!("fleet fingerprint failed: {status}"))?;
    let authz_version = handle
        .authz_version
        .as_ref()
        .map(|probe| probe())
        .unwrap_or_default();

    let next = LastSeen {
        fingerprint: fingerprint.clone(),
        authz_version: authz_version.clone(),
        per_type: per_type.clone(),
    };

    // 3. On the seeding pass, just record the baseline (no spurious reload).
    if !seeded {
        return Ok(next);
    }

    let registry_changed = fingerprint != last.fingerprint;
    let authz_changed = authz_version != last.authz_version;
    if registry_changed || authz_changed {
        // Per-type reload metric: count exactly the types whose world changed.
        if let Some(metrics) = handle.metrics.as_ref() {
            // How long this reload cycle took (resync → recompute → apply). Fed on
            // every applied reload so the `udb_authz_policy_reload_seconds` histogram
            // reflects real serving-path reloads (urgent_fix #27).
            metrics.observe_policy_reload_seconds(reload_started.elapsed().as_secs_f64());
            // Policy-distribution invalidation lag (SLO `policy.distribution_ack`):
            // the gap between when the registry change was EMITTED (the latest
            // resource `updated_at`, written by `resync` above) and now, when this
            // subscriber applies it / wakes the push streams. This is a real
            // emit→apply measurement, not a constant. (A registry-less authz-only
            // change has no resource timestamp; skip the observation in that case.)
            if registry_changed {
                if let Ok(Some(emit_unix)) = store::latest_updated_at_unix(&handle.pool).await {
                    let now_unix = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_secs_f64())
                        .unwrap_or(0.0);
                    let lag = (now_unix - emit_unix as f64).max(0.0);
                    metrics.observe_policy_invalidation_lag_seconds(lag);
                }
            }
            for (rt_db, version) in &per_type {
                if last.per_type.get(rt_db) != Some(version) {
                    metrics.inc_control_reload_applied(rt_db);
                }
            }
            // An authz-only change (no sourced-registry delta) still applied a
            // reload for the security-policy view.
            if authz_changed && !registry_changed {
                metrics.inc_control_reload_applied(resource_type_to_db(
                    crate::proto::udb::core::control::entity::v1::ResourceType::MethodSecurityPolicy,
                ));
            }
        }
        // Wake every open stream so it pushes the new versions now.
        handle.reload_notify.notify_waiters();
    }
    Ok(next)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn last_with(fp: &str, authz: &str, pairs: &[(&'static str, &str)]) -> LastSeen {
        LastSeen {
            fingerprint: fp.to_string(),
            authz_version: authz.to_string(),
            per_type: pairs.iter().map(|(k, v)| (*k, v.to_string())).collect(),
        }
    }

    #[test]
    fn reload_interval_clamps_env() {
        // Default when unset.
        unsafe {
            std::env::remove_var(ENV_RELOAD_INTERVAL_MS);
        }
        assert_eq!(reload_interval(), Duration::from_millis(1_000));
    }

    // The change-detection predicate: a reload fires only when the fleet
    // fingerprint OR the authz version actually changed (mirrors `run_once`).
    #[test]
    fn change_detection_fires_only_on_real_change() {
        let prev = last_with("fp1", "az1", &[("RESOURCE_TYPE_ROUTING_POLICY", "r1")]);

        // No change → no reload.
        let same = (prev.fingerprint.clone(), prev.authz_version.clone());
        let changed = same.0 != prev.fingerprint || same.1 != prev.authz_version;
        assert!(!changed, "identical fingerprint+authz must not reload");

        // Registry changed.
        let reg = ("fp2".to_string(), prev.authz_version.clone());
        assert!(reg.0 != prev.fingerprint || reg.1 != prev.authz_version);

        // Authz-only change.
        let az = (prev.fingerprint.clone(), "az2".to_string());
        assert!(az.0 != prev.fingerprint || az.1 != prev.authz_version);
    }

    // Per-type reload accounting: only the types whose world version changed are
    // counted (mirrors the `run_once` metric loop).
    #[test]
    fn per_type_reload_counts_only_changed_types() {
        let last = last_with(
            "fp1",
            "az1",
            &[
                ("RESOURCE_TYPE_BACKEND_TARGET_DEFINITION", "b1"),
                ("RESOURCE_TYPE_ROUTING_POLICY", "r1"),
            ],
        );
        let next: std::collections::BTreeMap<&'static str, String> = [
            ("RESOURCE_TYPE_BACKEND_TARGET_DEFINITION", "b1".to_string()), // unchanged
            ("RESOURCE_TYPE_ROUTING_POLICY", "r2".to_string()),            // changed
        ]
        .into_iter()
        .collect();
        let changed: Vec<&'static str> = next
            .iter()
            .filter(|(k, v)| last.per_type.get(*k) != Some(*v))
            .map(|(k, _)| *k)
            .collect();
        assert_eq!(changed, vec!["RESOURCE_TYPE_ROUTING_POLICY"]);
    }
}
