//! Shared singleton-worker leases for broker background tasks.
//!
//! The system catalog already owns `cdc.lock_log_relation()`: a compact
//! Postgres table with one row per lock key and a heartbeat timestamp. Reusing it
//! here keeps singleton coordination in the same HA control-plane store instead
//! of growing one-off worker locks in service modules.

use std::future::Future;
use std::time::Duration;

use sqlx::{PgPool, Row};

/// Pace periodic freshness checks and bounded worker passes without replaying
/// obsolete ticks after a slow pass or scheduler stall. The first tick remains
/// immediate; callers that already performed startup work can consume it.
pub(crate) fn periodic_worker_interval(period: Duration) -> tokio::time::Interval {
    let mut interval = tokio::time::interval(period);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    interval
}

pub const WORKER_CDC_POSTGRES_SOURCE: &str = "udb:cdc:source:postgres";
pub const WORKER_CDC_MYSQL_SOURCE: &str = "udb:cdc:source:mysql";
pub const WORKER_CDC_MONGODB_SOURCE: &str = "udb:cdc:source:mongodb";
pub const WORKER_STORAGE_ORPHAN_REAPER: &str = "udb:storage:orphan-reaper";
pub const WORKER_WEBRTC_STALE_PEER_REAPER: &str = "udb:webrtc:stale-peer-reaper";
pub const WORKER_PROJECTION_MATERIALIZER: &str = "udb:projection:materializer";
pub const WORKER_PROJECTION_RECONCILIATION: &str = "udb:projection:reconciliation";
pub const WORKER_XA_RECOVERY: &str = "udb:xa:recovery";
/// Leader-elected Vault database-credential lease reaper (master-plan 9.1).
/// Only the lease holder revokes and drops expired generated Postgres login
/// roles, then marks the durable VaultDbCredentialLease row REVOKED. The handler
/// never stores the password, so the lease row is the sole revocation source.
pub const WORKER_VAULT_LEASE_REAPER: &str = "udb:vault:lease-reaper";
/// Leader-elected scheduler tick (master-plan 9.3). Only the lease holder claims
/// DUE `scheduled_jobs` rows (`FOR UPDATE SKIP LOCKED`) and FIRES outbox events
/// (`udb.scheduler.job.fired.v1`), so a cron/one-shot job is never double-fired
/// across the cluster — the tick runs once per interval cluster-wide, not per node.
pub const WORKER_SCHEDULER_TICK: &str = "udb:scheduler:tick";
/// Leader-elected compliance-evidence export worker (master-plan 4.4). Only the
/// lease holder drains the durable audit window into the compliance-evidence
/// bucket, so a chain-hashed evidence bundle is produced exactly once per
/// interval across the cluster rather than once per node.
pub const WORKER_EVIDENCE_EXPORT: &str = "udb:compliance:evidence-export";
/// Leader-elected CDC-driven cache-invalidation worker (master-plan 9.6). Only the
/// lease holder consumes the tenant-scoped CDC change stream and SCAN+DEL-sweeps
/// the affected `udb:cache:<tenant>:<ns>:*` namespace, emitting
/// `udb.cache.invalidated.v1` exactly once per change cluster-wide instead of
/// every node racing to invalidate (and double-emitting). The consumer body is
/// `service::cache_service::run_cache_invalidation_once`.
pub const WORKER_CACHE_INVALIDATOR: &str = "udb:cache:invalidator";
/// Leader-elected webhook delivery worker (master-plan 9.4). Only the lease
/// holder consumes the tenant-scoped CDC change stream and POSTs each event to the
/// matching tenant's external endpoints (HMAC-signed, SSRF-revalidated at delivery
/// time), so an event is delivered once per (event, endpoint) cluster-wide instead
/// of every node racing to deliver (and double-POSTing). The consumer body is
/// `service::webhook_service::run_webhook_delivery_once`.
pub const WORKER_WEBHOOK_DELIVERY: &str = "udb:webhook:delivery";
/// Leader-elected embedding work emitter (master-plan 9.11). Only the lease
/// holder joins active embedding sources to durable CDC-journal source changes,
/// emits `udb.embedding.work.v1` once per source event for sidecar inference, and
/// deletes orphan vectors on source-row deletes via the shared vector seam.
pub const WORKER_EMBEDDING_WORK_EMITTER: &str = "udb:embedding:work-emitter";
/// Leader-elected notification delivery worker (master-plan 9.13). Only the
/// lease holder drains queued `NotificationLog` intents through the generic
/// provider HTTP adapter, decrypting wrapped credentials only at use and writing
/// terminal `NotificationDeliveryAttempt` rows through the shared ReportDelivery
/// writer shape.
pub const WORKER_NOTIFICATION_DELIVERY: &str = "udb:notification:delivery";
/// Leader-elected metering rollup worker (master-plan 9.9). Only the lease
/// holder aggregates closed `UsageEvent` windows and emits
/// `udb.metering.rollup.v1` outbox records for billing/export consumers. The
/// rollup id is deterministic per tenant/method/unit/window so retries are
/// deduped against both the outbox and CDC journal.
pub const WORKER_METERING_ROLLUP: &str = "udb:metering:rollup";
/// Leader-elected workflow tick worker (master-plan 9.12). Only the lease holder
/// claims due workflow instances (`FOR UPDATE SKIP LOCKED`) and advances durable
/// state + outbox transition atomically, so a workflow step is never advanced once
/// per replica. Compensation stays on the existing saga recovery worker; this
/// tick only emits transition events.
pub const WORKER_WORKFLOW_TICK: &str = "udb:workflow:tick";
/// Leader-elected Kafka-triggered asset-pipeline manager (master-plan 5.2). Only
/// the lease holder reconciles the distinct active `trigger_topic`s across
/// pipeline definitions, running exactly one consumer per topic cluster-wide
/// (start on definition-add, stop on definition-remove). A singleton avoids every
/// node racing to discover and spawn the same per-topic consumers; at-least-once
/// redelivery is made safe by the handler's idempotency (correlation_id = file_id),
/// not single-ownership. The consumer body is `asset_service::run_trigger_consumer`.
pub const WORKER_ASSET_TRIGGER_MANAGER: &str = "udb:asset:trigger-manager";
/// Leader-elected analytics percentile rollup (depth lane 16.1.5). Only the
/// lease holder recomputes p50/p95/p99 per (tenant, stage, hour) over the
/// trailing window into `pipeline_metric_snapshot` rows, so every replica
/// serves the same percentile columns instead of racing redundant UPDATEs.
/// The pass body is `service::analytics_service::run_analytics_rollup_once`.
pub const WORKER_ANALYTICS_ROLLUP: &str = "udb:analytics:rollup";
/// Leader-elected search index-freshness consumer (depth lane 16.2.1). Only the
/// lease holder joins published CDC-journal source changes to ACTIVE search
/// indexes and re-upserts changed rows through the mediated vector seam, deduped
/// by an applied-marker per source event. The pass body is
/// `service::search_service::run_index_freshness_consumer`.
pub const WORKER_SEARCH_FRESHNESS: &str = "udb:search:freshness";
/// Leader-elected search reindex/teardown worker (depth lane 16.2.2/16.2.3).
/// Only the lease holder consumes `reindex.requested` jobs (mediated source
/// re-read, engine re-upsert, `REINDEXING -> ACTIVE` writeback) and
/// `index.deleted` teardown jobs (engine point purge), so an index is rebuilt
/// exactly once cluster-wide. The pass body is
/// `service::search_service::run_search_reindex_once`.
pub const WORKER_SEARCH_REINDEX: &str = "udb:search:reindex";
/// Leader-elected lock expiry reaper (depth lane 16.5.1). Only the lease holder
/// flips lapsed `HELD` locks to `EXPIRED` (FOR UPDATE SKIP LOCKED) with the
/// `udb.lock.lock.expired.v1` event in the same transaction, so expired leases
/// stop counting against tenant quotas without double-expiring across replicas.
/// The pass body is `service::lock_service::run_lock_expiry_once`.
pub const WORKER_LOCK_EXPIRY_REAPER: &str = "udb:lock:expiry-reaper";
/// Leader-elected backup maintenance: enforces each enabled `BackupPolicy`'s
/// retention (prunes old runs + their objects) and fires due scheduled backups.
pub const WORKER_BACKUP_RETENTION: &str = "udb:backup:retention";

const MIN_LEASE_TTL_SECS: u64 = 5;
pub const WORKER_SINGLETON_LEASE_TTL: Duration = Duration::from_secs(30);
pub const WORKER_SINGLETON_RETRY_SLEEP: Duration = WORKER_SINGLETON_LEASE_TTL;

/// Phase 1 HA target for singleton background workers. All singleton workers use
/// the same lease floor, so the bounded failover target is measurable from the
/// lease TTL instead of living only in operations prose.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SingletonHaTarget {
    pub max_duplicate_winners: u8,
    pub max_failover_seconds: u64,
    pub recovery_point: &'static str,
}

pub const SINGLETON_HA_TARGET: SingletonHaTarget = SingletonHaTarget {
    max_duplicate_winners: 1,
    max_failover_seconds: WORKER_SINGLETON_LEASE_TTL.as_secs(),
    recovery_point: "last durable SystemStores/outbox/saga/2PC ledger commit",
};

#[derive(Debug, Clone)]
pub struct PostgresSingletonLease {
    pool: PgPool,
    relation: String,
    worker_name: String,
    lock_key: i64,
    owner_id: String,
    fencing_token: i64,
    ttl: Duration,
}

impl PostgresSingletonLease {
    pub async fn try_acquire(
        pool: PgPool,
        relation: impl Into<String>,
        worker_name: impl Into<String>,
        ttl: Duration,
    ) -> Result<Option<Self>, String> {
        let relation = relation.into();
        let worker_name = worker_name.into();
        let ttl = normalized_ttl(ttl);
        let lock_key = worker_lock_key(&worker_name);
        let owner_id = worker_owner_id(&worker_name);
        let sql = acquire_sql(&relation);
        let row = sqlx::query(&sql)
            .bind(lock_key)
            .bind(&owner_id)
            .bind(ttl.as_secs_f64())
            .fetch_optional(&pool)
            .await
            .map_err(|err| format!("acquire singleton lease {worker_name} failed: {err}"))?;
        let Some(row) = row else {
            return Ok(None);
        };
        let fencing_token = row.try_get::<i64, _>("fencing_token").unwrap_or(1);
        Ok(Some(Self {
            pool,
            relation,
            worker_name,
            lock_key,
            owner_id,
            fencing_token,
            ttl,
        }))
    }

    pub fn worker_name(&self) -> &str {
        &self.worker_name
    }

    pub fn owner_id(&self) -> &str {
        &self.owner_id
    }

    pub fn fencing_token(&self) -> i64 {
        self.fencing_token
    }

    /// A cheap, cloneable handle a singleton task uses to verify — immediately
    /// before each externally visible side effect — that this lease is still
    /// the current one (same holder, same fencing token, not expired). A paused
    /// or partitioned holder whose lease was taken over fails the check and
    /// must abandon the side effect instead of racing the new leader.
    pub fn fence(&self) -> LeaseFence {
        LeaseFence {
            pool: self.pool.clone(),
            relation: self.relation.clone(),
            worker_name: self.worker_name.clone(),
            lock_key: self.lock_key,
            owner_id: self.owner_id.clone(),
            fencing_token: self.fencing_token,
            ttl: self.ttl,
        }
    }

    pub async fn heartbeat(&self) -> Result<bool, String> {
        let sql = heartbeat_sql(&self.relation);
        let result = sqlx::query(&sql)
            .bind(self.lock_key)
            .bind(&self.owner_id)
            .bind(self.fencing_token)
            .bind(self.ttl.as_secs_f64())
            .execute(&self.pool)
            .await
            .map_err(|err| {
                format!(
                    "heartbeat singleton lease {} failed: {err}",
                    self.worker_name
                )
            })?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn release(&self) -> Result<(), String> {
        let sql = release_sql(&self.relation);
        sqlx::query(&sql)
            .bind(self.lock_key)
            .bind(&self.owner_id)
            .bind(self.fencing_token)
            .execute(&self.pool)
            .await
            .map_err(|err| format!("release singleton lease {} failed: {err}", self.worker_name))?;
        Ok(())
    }
}

/// Fencing handle for a held singleton lease (see [`PostgresSingletonLease::fence`]).
#[derive(Debug, Clone)]
pub struct LeaseFence {
    pool: PgPool,
    relation: String,
    worker_name: String,
    lock_key: i64,
    owner_id: String,
    fencing_token: i64,
    ttl: Duration,
}

impl LeaseFence {
    pub fn fencing_token(&self) -> i64 {
        self.fencing_token
    }

    pub fn worker_name(&self) -> &str {
        &self.worker_name
    }

    /// `Ok(())` only while this holder's fencing token is still the current
    /// one and the lease has not expired. Read-only (does not extend the
    /// lease). Call it right before every side effect a superseded leader
    /// must not perform.
    pub async fn check(&self) -> Result<(), String> {
        let sql = fence_check_sql(&self.relation);
        let current: Option<i32> = sqlx::query_scalar(&sql)
            .bind(self.lock_key)
            .bind(&self.owner_id)
            .bind(self.fencing_token)
            .bind(self.ttl.as_secs_f64())
            .fetch_optional(&self.pool)
            .await
            .map_err(|err| {
                format!(
                    "fencing check for singleton lease {} failed: {err}",
                    self.worker_name
                )
            })?;
        if current.is_some() {
            Ok(())
        } else {
            Err(fence_lost_message(&self.worker_name, self.fencing_token))
        }
    }
}

fn fence_lost_message(worker_name: &str, fencing_token: i64) -> String {
    format!(
        "singleton lease {worker_name} superseded (fencing token {fencing_token} is no longer \
         current); side effect abandoned"
    )
}

/// Run `task` once under the singleton lease. The lease is heartbeated while
/// the task runs (a long pass must not let the lease lapse and a peer take
/// over mid-pass); if the heartbeat finds the lease lost, the task is dropped
/// and an error is returned. Returns `Ok(None)` when a peer holds the lease.
pub async fn run_once<T, F, Fut>(
    pool: &PgPool,
    relation: &str,
    worker_name: &str,
    ttl: Duration,
    task: F,
) -> Result<Option<T>, String>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = T>,
{
    run_once_fenced(pool, relation, worker_name, ttl, |_fence| task()).await
}

/// [`run_once`] that hands the task a [`LeaseFence`], so the task can verify
/// its fencing token immediately before each side effect.
pub async fn run_once_fenced<T, F, Fut>(
    pool: &PgPool,
    relation: &str,
    worker_name: &str,
    ttl: Duration,
    task: F,
) -> Result<Option<T>, String>
where
    F: FnOnce(LeaseFence) -> Fut,
    Fut: Future<Output = T>,
{
    let Some(lease) =
        PostgresSingletonLease::try_acquire(pool.clone(), relation.to_string(), worker_name, ttl)
            .await?
    else {
        return Ok(None);
    };
    tracing::debug!(
        worker = lease.worker_name(),
        owner = lease.owner_id(),
        fencing_token = lease.fencing_token(),
        "singleton worker lease acquired"
    );
    let fence = lease.fence();
    let output = drive_with_heartbeat(&lease, ttl, task(fence)).await;
    if let Err(err) = lease.release().await {
        tracing::warn!(worker = worker_name, error = %err, "singleton worker lease release failed");
    }
    output.map(Some)
}

/// Poll `task` to completion while heartbeating `lease` every `ttl/3`
/// (clamped 1..=10s). A failed or lost heartbeat drops the task future and
/// returns an error: a holder that can no longer prove ownership must stop.
async fn drive_with_heartbeat<T, Fut>(
    lease: &PostgresSingletonLease,
    ttl: Duration,
    task: Fut,
) -> Result<T, String>
where
    Fut: Future<Output = T>,
{
    let mut heartbeat = periodic_worker_interval(heartbeat_period(ttl));
    // The first tick fires immediately; the lease was just acquired.
    heartbeat.tick().await;
    let mut task = Box::pin(task);
    loop {
        tokio::select! {
            result = &mut task => return Ok(result),
            _ = heartbeat.tick() => {
                if !lease.heartbeat().await? {
                    return Err(format!("singleton lease lost for {}", lease.worker_name()));
                }
            }
        }
    }
}

fn heartbeat_period(ttl: Duration) -> Duration {
    Duration::from_secs((normalized_ttl(ttl).as_secs() / 3).clamp(1, 10))
}

pub async fn run_while_leader<T, F, Fut>(
    pool: &PgPool,
    relation: &str,
    worker_name: &str,
    ttl: Duration,
    task: F,
) -> Result<Option<T>, String>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = T>,
{
    run_while_leader_fenced(pool, relation, worker_name, ttl, |_fence| task()).await
}

/// [`run_while_leader`] that hands the task a [`LeaseFence`] to verify before
/// each side effect.
pub async fn run_while_leader_fenced<T, F, Fut>(
    pool: &PgPool,
    relation: &str,
    worker_name: &str,
    ttl: Duration,
    task: F,
) -> Result<Option<T>, String>
where
    F: FnOnce(LeaseFence) -> Fut,
    Fut: Future<Output = T>,
{
    let Some(lease) =
        PostgresSingletonLease::try_acquire(pool.clone(), relation.to_string(), worker_name, ttl)
            .await?
    else {
        return Ok(None);
    };
    tracing::info!(
        worker = lease.worker_name(),
        owner = lease.owner_id(),
        fencing_token = lease.fencing_token(),
        "singleton worker lease acquired"
    );
    let fence = lease.fence();
    // A lost lease returns early WITHOUT releasing: the row now belongs to the
    // new holder (release is fenced on our token anyway).
    let output = drive_with_heartbeat(&lease, ttl, task(fence)).await?;
    if let Err(err) = lease.release().await {
        tracing::warn!(worker = worker_name, error = %err, "singleton worker lease release failed");
    }
    Ok(Some(output))
}

pub fn worker_lock_key(worker_name: &str) -> i64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in format!("udb:singleton:{worker_name}").as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    (hash & 0x7fff_ffff_ffff_ffff) as i64
}

fn worker_owner_id(worker_name: &str) -> String {
    let host = std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .unwrap_or_else(|_| "unknown-host".to_string());
    format!("{host}:{}:{worker_name}", std::process::id())
}

fn normalized_ttl(ttl: Duration) -> Duration {
    Duration::from_secs(ttl.as_secs().max(MIN_LEASE_TTL_SECS))
}

fn acquire_sql(relation: &str) -> String {
    format!(
        "INSERT INTO {relation} (lock_key, holder_host, acquired_at, fencing_token)
         VALUES ($1, $2, NOW(), 1)
         ON CONFLICT (lock_key) DO UPDATE
             SET holder_host = EXCLUDED.holder_host,
                 acquired_at = NOW(),
                 fencing_token = {relation}.fencing_token + 1
         WHERE {relation}.acquired_at < NOW() - make_interval(secs => $3::DOUBLE PRECISION)
         RETURNING fencing_token"
    )
}

fn heartbeat_sql(relation: &str) -> String {
    format!(
        "UPDATE {relation}
         SET acquired_at = NOW()
         WHERE lock_key = $1
           AND holder_host = $2
           AND fencing_token = $3
           AND acquired_at >= NOW() - make_interval(secs => $4::DOUBLE PRECISION)"
    )
}

fn fence_check_sql(relation: &str) -> String {
    format!(
        "SELECT 1::INT4 FROM {relation}
         WHERE lock_key = $1
           AND holder_host = $2
           AND fencing_token = $3
           AND acquired_at >= NOW() - make_interval(secs => $4::DOUBLE PRECISION)"
    )
}

fn release_sql(relation: &str) -> String {
    format!(
        "UPDATE {relation}
         SET acquired_at = '1970-01-01'
         WHERE lock_key = $1 AND holder_host = $2 AND fencing_token = $3"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;

    #[tokio::test(start_paused = true)]
    async fn periodic_worker_interval_does_not_replay_missed_passes() {
        let period = Duration::from_millis(100);
        let started = tokio::time::Instant::now();
        let mut tick = periodic_worker_interval(period);
        tick.tick().await;
        assert_eq!(
            tokio::time::Instant::now(),
            started,
            "startup stays immediate"
        );

        // A pass or runtime stall spans several cadences. One overdue pass is
        // still due, but the elapsed periods must not become extra ready work.
        tokio::time::advance(period * 8 + Duration::from_millis(17)).await;
        tick.tick().await;
        let resumed = tokio::time::Instant::now();
        assert!(tick.tick().now_or_never().is_none(), "no catch-up burst");
        tokio::time::advance(period - Duration::from_millis(1)).await;
        assert!(
            tick.tick().now_or_never().is_none(),
            "a full cadence is owed"
        );
        tokio::time::advance(Duration::from_millis(1)).await;
        tick.tick().await;
        assert_eq!(tokio::time::Instant::now(), resumed + period);
    }

    #[tokio::test(start_paused = true)]
    async fn periodic_worker_interval_retains_deadline_across_cancelled_waits() {
        let period = Duration::from_millis(100);
        let started = tokio::time::Instant::now();
        let mut tick = periodic_worker_interval(period);
        tick.tick().await;

        // Heartbeats and metrics share a select loop with other ready work.
        // Dropping the pending tick future must not postpone its deadline.
        for _ in 0..4 {
            tokio::time::advance(Duration::from_millis(20)).await;
            assert!(tick.tick().now_or_never().is_none());
        }
        tokio::time::advance(Duration::from_millis(20)).await;
        tick.tick().await;
        assert_eq!(tokio::time::Instant::now(), started + period);
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct LeaseState {
        owner_id: String,
        acquired_at_ms: i64,
        fencing_token: i64,
    }

    fn simulate_acquire(
        state: Option<LeaseState>,
        owner_id: &str,
        now_ms: i64,
        ttl_ms: i64,
    ) -> (Option<LeaseState>, bool) {
        match state {
            None => (
                Some(LeaseState {
                    owner_id: owner_id.to_string(),
                    acquired_at_ms: now_ms,
                    fencing_token: 1,
                }),
                true,
            ),
            Some(current)
                if current.owner_id == owner_id
                    || current.acquired_at_ms < now_ms.saturating_sub(ttl_ms) =>
            {
                let next = LeaseState {
                    owner_id: owner_id.to_string(),
                    acquired_at_ms: now_ms,
                    fencing_token: current.fencing_token + 1,
                };
                (Some(next), true)
            }
            Some(current) => (Some(current), false),
        }
    }

    #[test]
    fn worker_lock_keys_are_stable_and_distinct() {
        assert_eq!(
            worker_lock_key(WORKER_XA_RECOVERY),
            worker_lock_key(WORKER_XA_RECOVERY)
        );
        assert_ne!(
            worker_lock_key(WORKER_XA_RECOVERY),
            worker_lock_key(WORKER_STORAGE_ORPHAN_REAPER)
        );
        assert_ne!(
            worker_lock_key(WORKER_STORAGE_ORPHAN_REAPER),
            worker_lock_key(WORKER_WEBRTC_STALE_PEER_REAPER)
        );
        assert_ne!(
            worker_lock_key(WORKER_PROJECTION_MATERIALIZER),
            worker_lock_key(WORKER_PROJECTION_RECONCILIATION)
        );
        assert_eq!(
            worker_lock_key(WORKER_EVIDENCE_EXPORT),
            worker_lock_key(WORKER_EVIDENCE_EXPORT)
        );
        assert_ne!(
            worker_lock_key(WORKER_EVIDENCE_EXPORT),
            worker_lock_key(WORKER_XA_RECOVERY)
        );
    }

    #[test]
    fn lease_ttl_has_lower_bound() {
        assert_eq!(normalized_ttl(Duration::from_secs(0)).as_secs(), 5);
        assert_eq!(
            normalized_ttl(WORKER_SINGLETON_LEASE_TTL).as_secs(),
            WORKER_SINGLETON_LEASE_TTL.as_secs()
        );
    }

    #[test]
    fn singleton_ha_target_is_bounded_by_lease_floor() {
        assert_eq!(SINGLETON_HA_TARGET.max_duplicate_winners, 1);
        assert_eq!(
            SINGLETON_HA_TARGET.max_failover_seconds,
            WORKER_SINGLETON_LEASE_TTL.as_secs()
        );
        assert!(SINGLETON_HA_TARGET.recovery_point.contains("durable"));
    }

    #[test]
    fn lease_sql_prevents_split_brain_and_fences_stale_owners() {
        let acquire = acquire_sql("udb_system.cdc_lock_log");
        assert!(
            acquire.contains("ON CONFLICT"),
            "acquire must be atomic per lock key"
        );
        assert!(
            acquire.contains("fencing_token = udb_system.cdc_lock_log.fencing_token + 1"),
            "takeover must advance the fencing token"
        );
        assert!(
            acquire.contains("acquired_at < NOW() - make_interval"),
            "takeover must only happen after lease expiry"
        );

        let heartbeat = heartbeat_sql("udb_system.cdc_lock_log");
        assert!(heartbeat.contains("holder_host = $2"));
        assert!(heartbeat.contains("fencing_token = $3"));
        assert!(heartbeat.contains("acquired_at >= NOW() - make_interval"));

        let release = release_sql("udb_system.cdc_lock_log");
        assert!(release.contains("holder_host = $2"));
        assert!(release.contains("fencing_token = $3"));
    }

    #[test]
    fn fence_check_is_read_only_and_rejects_superseded_or_expired_holders() {
        let sql = fence_check_sql("udb_system.cdc_lock_log");
        assert!(
            sql.trim_start().starts_with("SELECT"),
            "fence check must not mutate"
        );
        assert!(!sql.contains("UPDATE"));
        assert!(sql.contains("holder_host = $2"));
        assert!(
            sql.contains("fencing_token = $3"),
            "a takeover advances the token, so the old holder's check fails"
        );
        assert!(
            sql.contains("acquired_at >= NOW() - make_interval"),
            "an expired lease must fail the check even before takeover"
        );
        let msg = fence_lost_message(WORKER_XA_RECOVERY, 7);
        assert!(msg.contains("superseded") && msg.contains('7'));
    }

    #[test]
    fn heartbeat_period_is_a_third_of_ttl_and_clamped() {
        assert_eq!(heartbeat_period(Duration::from_secs(30)).as_secs(), 10);
        assert_eq!(heartbeat_period(Duration::from_secs(12)).as_secs(), 4);
        // Floor: TTL is normalized to >= 5s, period never below 1s.
        assert_eq!(heartbeat_period(Duration::from_secs(0)).as_secs(), 1);
        // Ceiling: long TTLs still heartbeat at least every 10s.
        assert_eq!(heartbeat_period(Duration::from_secs(600)).as_secs(), 10);
    }

    #[tokio::test]
    #[ignore = "requires Postgres; set UDB_INTEGRATION_PG_DSN and run with --ignored"]
    async fn fence_check_fails_after_takeover_live() {
        let Ok(dsn) = std::env::var("UDB_INTEGRATION_PG_DSN") else {
            eprintln!("skipped: set UDB_INTEGRATION_PG_DSN");
            return;
        };
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)
            .connect(&dsn)
            .await
            .expect("connect live postgres");
        crate::runtime::system::ensure_system_catalog(&pool)
            .await
            .expect("ensure system catalog");
        let relation = crate::runtime::cdc::CdcConfig::current().lock_log_relation();
        let worker = format!("udb:test:fence:{}", uuid::Uuid::new_v4());
        let lease = PostgresSingletonLease::try_acquire(
            pool.clone(),
            relation.clone(),
            worker.clone(),
            Duration::from_secs(5),
        )
        .await
        .expect("acquire")
        .expect("free lease");
        let fence = lease.fence();
        fence.check().await.expect("fresh holder passes the fence");
        // Simulate expiry + takeover by a peer: the token advances.
        sqlx::query(&format!(
            "UPDATE {relation} SET acquired_at = NOW() - INTERVAL '1 hour' WHERE lock_key = $1"
        ))
        .bind(worker_lock_key(&worker))
        .execute(&pool)
        .await
        .expect("age lease");
        let peer = PostgresSingletonLease::try_acquire(
            pool.clone(),
            relation.clone(),
            worker.clone(),
            Duration::from_secs(5),
        )
        .await
        .expect("peer acquire")
        .expect("expired lease is taken over");
        assert!(peer.fencing_token() > lease.fencing_token());
        let err = fence
            .check()
            .await
            .expect_err("superseded holder must fail");
        assert!(err.contains("superseded"), "{err}");
        peer.release().await.expect("release");
    }

    #[test]
    fn lease_state_model_has_no_double_winner_before_expiry() {
        let (state, broker_a) = simulate_acquire(None, "broker-a", 1_000, 5_000);
        assert!(broker_a);
        let (state, broker_b) = simulate_acquire(state, "broker-b", 1_000, 5_000);
        assert!(
            !broker_b,
            "second broker must not win while the lease is live"
        );
        let state = state.expect("lease state");
        assert_eq!(state.owner_id, "broker-a");
        assert_eq!(state.fencing_token, 1);
    }

    #[test]
    fn lease_state_model_failover_requires_expiry_and_advances_fence() {
        let (state, broker_a) = simulate_acquire(None, "broker-a", 1_000, 5_000);
        assert!(broker_a);
        let (state, broker_b_early) = simulate_acquire(state, "broker-b", 5_999, 5_000);
        assert!(!broker_b_early, "takeover before TTL expiry must fail");
        let (state, broker_b_after_expiry) = simulate_acquire(state, "broker-b", 6_001, 5_000);
        assert!(broker_b_after_expiry);
        let state = state.expect("lease state");
        assert_eq!(state.owner_id, "broker-b");
        assert_eq!(state.fencing_token, 2);
    }
}
