//! Shared singleton-worker leases for broker background tasks.
//!
//! The system catalog already owns `cdc.lock_log_relation()`: a compact
//! Postgres table with one row per lock key and a heartbeat timestamp. Reusing it
//! here keeps singleton coordination in the same HA control-plane store instead
//! of growing one-off worker locks in service modules.

use std::future::Future;
use std::time::Duration;

use sqlx::{PgPool, Row};

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
    let output = task().await;
    if let Err(err) = lease.release().await {
        tracing::warn!(worker = worker_name, error = %err, "singleton worker lease release failed");
    }
    Ok(Some(output))
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
    let heartbeat_secs = (normalized_ttl(ttl).as_secs() / 3).clamp(1, 10);
    let mut heartbeat = tokio::time::interval(Duration::from_secs(heartbeat_secs));
    let mut task = Box::pin(task());
    let output = loop {
        tokio::select! {
            result = &mut task => break result,
            _ = heartbeat.tick() => {
                if !lease.heartbeat().await? {
                    return Err(format!("singleton lease lost for {worker_name}"));
                }
            }
        }
    };
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
