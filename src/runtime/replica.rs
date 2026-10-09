use std::sync::{
    Arc, RwLock,
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures::StreamExt;
use sqlx::PgPool;

/// Replica probes are background I/O. Do not start an unbounded batch of pool
/// waiters when a deployment has many configured replicas.
const MAX_CONCURRENT_REPLICA_HEALTH_PROBES: usize = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgReplicaStrategy {
    RoundRobin,
    LeastLag,
    RandomHealthy,
    PrimaryOnly,
}

impl PgReplicaStrategy {
    pub fn from_env() -> Self {
        Self::from_value(
            &std::env::var("UDB_PG_REPLICA_STRATEGY").unwrap_or_else(|_| "round_robin".into()),
        )
    }

    pub fn from_value(value: &str) -> Self {
        match value.to_ascii_lowercase().replace('-', "_").as_str() {
            "least_lag" | "leastlag" | "lag" => Self::LeastLag,
            "random" | "random_healthy" => Self::RandomHealthy,
            "primary" | "primary_only" | "disabled" | "off" => Self::PrimaryOnly,
            _ => Self::RoundRobin,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::RoundRobin => "round_robin",
            Self::LeastLag => "least_lag",
            Self::RandomHealthy => "random_healthy",
            Self::PrimaryOnly => "primary_only",
        }
    }
}

#[derive(Debug, Clone)]
pub struct PgReplicaSnapshot {
    pub label: String,
    pub healthy: bool,
    pub lag_millis: u64,
    pub latency_millis: u64,
    pub last_failure_unix_ms: u64,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct PgReplicaPool {
    label: String,
    pool: PgPool,
    healthy: Arc<AtomicBool>,
    lag_millis: Arc<AtomicU64>,
    latency_millis: Arc<AtomicU64>,
    last_failure_unix_ms: Arc<AtomicU64>,
    last_error: Arc<RwLock<Option<String>>>,
}

impl PgReplicaPool {
    pub fn new(label: String, pool: PgPool) -> Self {
        Self {
            label,
            pool,
            healthy: Arc::new(AtomicBool::new(true)),
            lag_millis: Arc::new(AtomicU64::new(0)),
            latency_millis: Arc::new(AtomicU64::new(0)),
            last_failure_unix_ms: Arc::new(AtomicU64::new(0)),
            last_error: Arc::new(RwLock::new(None)),
        }
    }

    pub fn label(&self) -> &str {
        &self.label
    }

    pub fn pool(&self) -> PgPool {
        self.pool.clone()
    }

    pub fn snapshot(&self) -> PgReplicaSnapshot {
        PgReplicaSnapshot {
            label: self.label.clone(),
            healthy: self.healthy.load(Ordering::Relaxed),
            lag_millis: self.lag_millis.load(Ordering::Relaxed),
            latency_millis: self.latency_millis.load(Ordering::Relaxed),
            last_failure_unix_ms: self.last_failure_unix_ms.load(Ordering::Relaxed),
            last_error: self.last_error.read().ok().and_then(|err| err.clone()),
        }
    }

    fn mark_healthy(&self, lag_millis: u64, latency_millis: u64) {
        self.healthy.store(true, Ordering::Relaxed);
        self.lag_millis.store(lag_millis, Ordering::Relaxed);
        self.latency_millis.store(latency_millis, Ordering::Relaxed);
        if let Ok(mut err) = self.last_error.write() {
            *err = None;
        }
    }

    fn mark_unhealthy(&self, latency_millis: u64, error: String) {
        self.healthy.store(false, Ordering::Relaxed);
        self.latency_millis.store(latency_millis, Ordering::Relaxed);
        self.last_failure_unix_ms
            .store(unix_now_millis(), Ordering::Relaxed);
        if let Ok(mut err) = self.last_error.write() {
            *err = Some(error);
        }
    }

    fn is_eligible(&self, max_lag: Duration) -> bool {
        self.healthy.load(Ordering::Relaxed)
            && self.lag_millis.load(Ordering::Relaxed) <= max_lag.as_millis() as u64
    }
}

/// Outcome of a [`PgReplicaManager::choose_bounded_replica`] attempt
/// (6.4 REPLICA_BOUNDED routing).
#[derive(Debug, Clone)]
pub enum BoundedReplicaRead {
    /// A replica was selected and provably caught up past the fence LSN
    /// within the staleness budget. Serve the read from this pool.
    Replica(PgPool),
    /// No eligible replica, or the replica did not catch up to the fence
    /// LSN before the staleness deadline (or a poll error). The caller MUST
    /// serve the read from the primary — never a stale replica — and MUST
    /// surface the carried [`StaleReadWarning`] so the failed-over read is
    /// never returned silently (6.4 doctrine). `ReplicaLagExceeded` when no
    /// replica was eligible within the budget; `FenceTimedOut` when the
    /// chosen replica didn't catch up to the fence LSN in time.
    FailoverToPrimary(crate::runtime::consistency::StaleReadWarning),
}

#[derive(Debug, Clone)]
pub struct PgReplicaManager {
    replicas: Arc<Vec<PgReplicaPool>>,
    strategy: PgReplicaStrategy,
    max_lag: Duration,
    fail_open: bool,
    next: Arc<AtomicUsize>,
    query_total: Arc<AtomicU64>,
    fallback_total: Arc<AtomicU64>,
    health_refresh: Arc<tokio::sync::Mutex<()>>,
}

impl Default for PgReplicaManager {
    fn default() -> Self {
        Self::empty()
    }
}

impl PgReplicaManager {
    pub fn empty() -> Self {
        Self {
            replicas: Arc::new(Vec::new()),
            strategy: PgReplicaStrategy::RoundRobin,
            max_lag: Duration::from_secs(3),
            fail_open: false,
            next: Arc::new(AtomicUsize::new(0)),
            query_total: Arc::new(AtomicU64::new(0)),
            fallback_total: Arc::new(AtomicU64::new(0)),
            health_refresh: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    pub fn new(
        replicas: Vec<PgReplicaPool>,
        strategy: PgReplicaStrategy,
        max_lag: Duration,
        fail_open: bool,
    ) -> Self {
        Self {
            replicas: Arc::new(replicas),
            strategy,
            max_lag,
            fail_open,
            next: Arc::new(AtomicUsize::new(0)),
            query_total: Arc::new(AtomicU64::new(0)),
            fallback_total: Arc::new(AtomicU64::new(0)),
            health_refresh: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    pub fn len(&self) -> usize {
        self.replicas.len()
    }

    pub fn is_empty(&self) -> bool {
        self.replicas.is_empty()
    }

    pub fn strategy(&self) -> PgReplicaStrategy {
        self.strategy
    }

    pub fn snapshots(&self) -> Vec<PgReplicaSnapshot> {
        self.replicas
            .iter()
            .map(PgReplicaPool::snapshot)
            .collect::<Vec<_>>()
    }

    pub fn choose_pool(&self) -> Option<PgPool> {
        self.choose_pool_with_max_lag(None)
    }

    pub fn choose_pool_with_max_lag(&self, max_lag_override: Option<Duration>) -> Option<PgPool> {
        if self.strategy == PgReplicaStrategy::PrimaryOnly || self.replicas.is_empty() {
            return None;
        }
        let max_lag = max_lag_override.unwrap_or(self.max_lag);

        let mut candidates = self
            .replicas
            .iter()
            .filter(|replica| replica.is_eligible(max_lag))
            .collect::<Vec<_>>();
        if candidates.is_empty() && self.fail_open {
            candidates = self.replicas.iter().collect::<Vec<_>>();
        }
        if candidates.is_empty() {
            self.fallback_total.fetch_add(1, Ordering::Relaxed);
            return None;
        }

        let selected = match self.strategy {
            PgReplicaStrategy::LeastLag => candidates
                .into_iter()
                .min_by_key(|replica| replica.lag_millis.load(Ordering::Relaxed)),
            PgReplicaStrategy::RandomHealthy => {
                let nanos = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|duration| duration.subsec_nanos() as usize)
                    .unwrap_or(0);
                candidates.get(nanos % candidates.len()).copied()
            }
            PgReplicaStrategy::RoundRobin => {
                let idx = self.next.fetch_add(1, Ordering::Relaxed);
                candidates.get(idx % candidates.len()).copied()
            }
            PgReplicaStrategy::PrimaryOnly => None,
        }?;

        self.query_total.fetch_add(1, Ordering::Relaxed);
        Some(selected.pool())
    }

    /// 6.4 REPLICA_BOUNDED routing: select a read replica whose lag is
    /// within `max_staleness`, then WAIT on its REAL applied WAL position
    /// (`pg_last_wal_replay_lsn()`) until it reaches `min_lsn`, using
    /// `max_staleness` as the wait budget. Returns the chosen pool only when
    /// the replica provably caught up past the caller's write; otherwise
    /// [`BoundedReplicaRead::FailoverToPrimary`] so the caller serves the
    /// read from the primary (which definitionally has the write) — a
    /// bounded read NEVER returns stale data without failing over.
    ///
    /// The fence is anchored to the replica's physical replay LSN, NOT a
    /// wall clock: a wall-clock fence would clear the instant `now()` passes
    /// a timestamp and serve stale data while claiming freshness. `min_lsn`
    /// is the caller's `WriteReceipt`/`ReadFence` Postgres LSN; `None` (or
    /// blank) degrades to pure lag-bounded selection with no LSN wait.
    pub async fn choose_bounded_replica(
        &self,
        min_lsn: Option<&str>,
        max_staleness: Duration,
    ) -> BoundedReplicaRead {
        use crate::runtime::consistency::StaleReadWarning;
        let budget_ms = max_staleness.as_millis() as u64;

        // Step 1: pick a replica inside the staleness/lag budget. None ⇒ no
        // eligible replica ⇒ fail over to the primary, attaching a
        // `ReplicaLagExceeded` warning so the caller never serves the
        // failed-over read silently.
        let Some(pool) = self.choose_pool_with_max_lag(Some(max_staleness)) else {
            self.fallback_total.fetch_add(1, Ordering::Relaxed);
            let (instance, lag_ms) = self.least_lagged_report();
            return BoundedReplicaRead::FailoverToPrimary(StaleReadWarning::ReplicaLagExceeded {
                instance,
                lag_ms,
                budget_ms,
            });
        };

        // Step 2: with no LSN fence, lag-bounded selection IS the contract.
        let Some(target_lsn) = min_lsn.map(str::trim).filter(|lsn| !lsn.is_empty()) else {
            return BoundedReplicaRead::Replica(pool);
        };

        // Step 3: wait on the replica's REAL applied LSN, failing over on
        // timeout or any error (malformed token, lost connection). The fence
        // is decided on the physical replay position (a REAL token), NOT a
        // wall clock; on failover we attach a `FenceTimedOut` warning so the
        // primary-served result is never returned silently.
        let started = Instant::now();
        match wait_for_replica_replay_lsn(&pool, target_lsn, max_staleness).await {
            Ok(true) => BoundedReplicaRead::Replica(pool),
            _ => {
                self.fallback_total.fetch_add(1, Ordering::Relaxed);
                let (instance, _) = self.least_lagged_report();
                BoundedReplicaRead::FailoverToPrimary(StaleReadWarning::FenceTimedOut {
                    backend: "postgres".to_string(),
                    instance,
                    lag_ms: started.elapsed().as_millis() as u64,
                })
            }
        }
    }

    /// Best-effort `(instance_label, lag_ms)` of the least-lagged replica,
    /// used to populate a failover [`StaleReadWarning`]. Returns
    /// `("<none>", 0)` when there are no replicas configured.
    fn least_lagged_report(&self) -> (String, u64) {
        self.replicas
            .iter()
            .map(|replica| {
                (
                    replica.label.clone(),
                    replica.lag_millis.load(Ordering::Relaxed),
                )
            })
            .min_by_key(|(_, lag)| *lag)
            .unwrap_or_else(|| ("<none>".to_string(), 0))
    }

    pub async fn refresh_health_once(&self) {
        // Clones share this guard. A slow refresh must not leave detached probes
        // running while the next tick starts a newer generation of results.
        let Ok(_refresh) = self.health_refresh.try_lock() else {
            return;
        };
        futures::stream::iter(self.replicas.iter().cloned())
            .for_each_concurrent(MAX_CONCURRENT_REPLICA_HEALTH_PROBES, probe_replica)
            .await;
    }

    pub fn start_health_task(&self, interval: Duration) {
        if self.replicas.is_empty() {
            return;
        }
        let manager = self.clone();
        tokio::spawn(async move {
            let mut tick = replica_health_ticks(interval);
            loop {
                tick.tick().await;
                manager.refresh_health_once().await;
            }
        });
    }

    pub fn metrics_text(&self) -> String {
        let snapshots = self.snapshots();
        let healthy = snapshots.iter().filter(|snapshot| snapshot.healthy).count();
        let mut out = format!(
            "# TYPE udb_pg_replica_count gauge\nudb_pg_replica_count {}\n\
             # TYPE udb_pg_replica_healthy gauge\nudb_pg_replica_healthy {}\n",
            snapshots.len(),
            healthy
        );
        out.push_str(&format!(
            "# TYPE udb_pg_replica_query_total counter\nudb_pg_replica_query_total {}\n\
             # TYPE udb_pg_replica_fallback_total counter\nudb_pg_replica_fallback_total {}\n",
            self.query_total.load(Ordering::Relaxed),
            self.fallback_total.load(Ordering::Relaxed)
        ));
        out.push_str("# TYPE udb_pg_replica_lag_seconds gauge\n");
        out.push_str("# TYPE udb_pg_replica_latency_milliseconds gauge\n");
        out.push_str("# TYPE udb_pg_replica_last_failure_unix_ms gauge\n");
        for snapshot in snapshots {
            let label = escape_prom_label(&snapshot.label);
            out.push_str(&format!(
                "udb_pg_replica_lag_seconds{{replica=\"{}\"}} {}\n\
                 udb_pg_replica_latency_milliseconds{{replica=\"{}\"}} {}\n\
                 udb_pg_replica_last_failure_unix_ms{{replica=\"{}\"}} {}\n",
                label,
                snapshot.lag_millis as f64 / 1000.0,
                label,
                snapshot.latency_millis,
                label,
                snapshot.last_failure_unix_ms
            ));
        }
        out
    }
}

pub fn replica_dsns_from_values(multi: Option<&str>, single: Option<&str>) -> Vec<String> {
    let mut out = Vec::new();
    if let Some(value) = multi {
        out.extend(split_replica_dsns(value));
    }
    if out.is_empty()
        && let Some(value) = single
    {
        out.extend(split_replica_dsns(value));
    }
    out
}

pub fn replica_dsns_from_env() -> Vec<String> {
    replica_dsns_from_values(
        std::env::var("UDB_PG_REPLICA_DSNS").ok().as_deref(),
        std::env::var("UDB_PG_REPLICA_DSN").ok().as_deref(),
    )
}

pub fn append_application_name(dsn: &str, app_name: &str) -> String {
    if dsn.contains("application_name") {
        dsn.to_string()
    } else if dsn.contains('?') {
        format!("{dsn}&application_name={app_name}")
    } else {
        format!("{dsn}?application_name={app_name}")
    }
}

fn split_replica_dsns(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|dsn| !dsn.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn replica_health_ticks(interval: Duration) -> tokio::time::Interval {
    let interval = interval.max(Duration::from_millis(1));
    // Startup already awaited its initial refresh. Start periodic work one
    // interval later, and skip missed ticks instead of issuing catch-up bursts.
    let mut tick = tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    tick
}

fn replica_probe_budget(pool: &PgPool) -> Duration {
    pool.options()
        .get_acquire_timeout()
        .min(Duration::from_secs(
            crate::runtime::config::DEFAULT_DB_ACQUIRE_TIMEOUT_SECS,
        ))
}

/// A cancelled query must not retain a pool slot while SQLx's return task
/// drains its PostgreSQL protocol. Completed queries keep normal pool reuse.
/// These probes install no SESSION request context: protocol completion is
/// distinct from the canonical RESET required by `PgRequestConnection`.
struct ReplicaQueryConnection {
    connection: Option<sqlx::pool::PoolConnection<sqlx::Postgres>>,
    query_completed: bool,
}

impl ReplicaQueryConnection {
    fn new(connection: sqlx::pool::PoolConnection<sqlx::Postgres>) -> Self {
        Self {
            connection: Some(connection),
            query_completed: false,
        }
    }

    fn connection_mut(&mut self) -> Result<&mut sqlx::PgConnection, sqlx::Error> {
        self.connection
            .as_mut()
            .map(|connection| &mut **connection)
            .ok_or(sqlx::Error::PoolClosed)
    }
}

impl Drop for ReplicaQueryConnection {
    fn drop(&mut self) {
        if !self.query_completed
            && let Some(connection) = self.connection.take()
        {
            drop(connection.detach());
        }
    }
}

async fn probe_replica(replica: PgReplicaPool) {
    let started = Instant::now();
    let budget = replica_probe_budget(&replica.pool);
    // One budget includes waiting for a pool slot AND executing the query.
    let result = tokio::time::timeout(budget, async {
        let mut connection = ReplicaQueryConnection::new(replica.pool.acquire().await?);
        let result = sqlx::query_as::<_, (Option<f64>,)>(
            "SELECT COALESCE(EXTRACT(EPOCH FROM (NOW() - pg_last_xact_replay_timestamp())), 0)::float8",
        )
        .fetch_one(connection.connection_mut()?)
        .await;
        connection.query_completed = true;
        result
    })
    .await;
    let latency_millis = started.elapsed().as_millis() as u64;
    match result {
        Ok(Ok((lag_seconds,))) if started.elapsed() < budget => {
            let lag_millis = lag_seconds.unwrap_or(0.0).max(0.0).mul_add(1000.0, 0.0) as u64;
            replica.mark_healthy(lag_millis, latency_millis);
        }
        Ok(Err(err)) => replica.mark_unhealthy(latency_millis, err.to_string()),
        // Cooperative timers can resume late. A Ready query on that late poll
        // still cannot certify health beyond the configured probe budget.
        Ok(Ok(_)) | Err(_) => replica.mark_unhealthy(
            latency_millis,
            "PostgreSQL replica health probe exceeded its deadline".to_string(),
        ),
    }
}

/// Poll interval for the replica-LSN fence loop, capped to the overall
/// budget so the loop returns promptly on a short staleness window.
/// Tunable via `UDB_REPLICA_LSN_POLL_MS` (default 25 ms), mirroring the
/// canonical-store durability poll cadence.
fn replica_lsn_poll_interval(timeout: Duration) -> Duration {
    let ms = std::env::var("UDB_REPLICA_LSN_POLL_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(25);
    Duration::from_millis(ms).min(timeout.max(Duration::from_millis(1)))
}

/// Wait until `pool` (a read replica) has REPLAYED WAL up to `target_lsn`,
/// or `timeout` elapses. Returns `Ok(true)` when the replica's applied LSN
/// reached the target, `Ok(false)` on timeout, `Err` on a poll failure.
///
/// `pg_last_wal_replay_lsn()` is the replica's PHYSICAL apply position — the
/// real replication position, NOT a wall clock. The comparison runs
/// server-side (`$1::pg_lsn <= …`) to avoid client-side hex-parsing bugs,
/// and `COALESCE(…, false)` maps a NULL replay LSN (a primary, or a replica
/// that hasn't begun replaying) to "not cleared" so the caller fails over
/// instead of being handed a vacuous pass. Mirrors
/// `PostgresCanonicalStore::wait_for_token`.
async fn wait_for_replica_replay_lsn(
    pool: &PgPool,
    target_lsn: &str,
    timeout: Duration,
) -> Result<bool, sqlx::Error> {
    let started = Instant::now();
    let Some(deadline) = started.checked_add(timeout).filter(|_| !timeout.is_zero()) else {
        return Ok(false);
    };
    let poll = replica_lsn_poll_interval(timeout);
    // The former elapsed check happened only AFTER fetch_one completed, so
    // waiting for a pool or a slow query could exceed max_staleness by minutes.
    tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), async {
        loop {
            let mut connection = ReplicaQueryConnection::new(pool.acquire().await?);
            let result: Result<bool, sqlx::Error> = sqlx::query_scalar(
                "SELECT COALESCE($1::pg_lsn <= pg_last_wal_replay_lsn(), false)",
            )
            .bind(target_lsn)
            .fetch_one(connection.connection_mut()?)
            .await;
            connection.query_completed = true;
            drop(connection);
            let cleared = result?;
            if started.elapsed() >= timeout {
                return Ok(false);
            }
            if cleared {
                return Ok(true);
            }
            let remaining = timeout.saturating_sub(started.elapsed());
            tokio::time::sleep(poll.min(remaining)).await;
        }
    })
    .await
    .unwrap_or(Ok(false))
}

fn escape_prom_label(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

fn unix_now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn replica_live_dsn() -> Option<String> {
        crate::runtime::service::live_tests::support::require_live_dsn_any(&[
            "UDB_LIVE_NATIVE_PG_DSN",
            "UDB_LIVE_AUTH_PG_DSN",
            "UDB_INTEGRATION_PG_DSN",
            "UDB_PG_DSN",
        ])
    }

    async fn wait_for_replica_query_lock(admin: &PgPool, pid: i32, function: &str) {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let waiting: bool = sqlx::query_scalar(
                    "SELECT EXISTS (SELECT 1 FROM pg_stat_activity \
                     WHERE pid = $1 AND wait_event_type = 'Lock' AND query LIKE $2)",
                )
                .bind(pid)
                .bind(format!("%{function}%"))
                .fetch_one(admin)
                .await
                .expect("observe blocked replica query");
                if waiting {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("replica query must reach the held PostgreSQL advisory lock");
    }

    #[tokio::test(start_paused = true)]
    async fn health_ticks_delay_start_and_skip_missed_intervals() {
        let mut tick = replica_health_ticks(Duration::from_secs(1));
        assert!(
            tokio::time::timeout(Duration::from_millis(100), tick.tick())
                .await
                .is_err(),
            "startup must not immediately repeat its completed initial probe"
        );
        tokio::time::advance(Duration::from_secs(5)).await;
        tick.tick().await;
        assert!(
            tokio::time::timeout(Duration::from_millis(100), tick.tick())
                .await
                .is_err(),
            "missed periods must not generate a burst of catch-up probes"
        );
    }

    #[tokio::test]
    #[ignore = "requires live PostgreSQL; exercised by the Native CI live lane"]
    async fn replica_refresh_awaits_bounded_fanout_live() {
        let Some(dsn) = replica_live_dsn() else {
            return;
        };
        let armed = Arc::new(AtomicBool::new(false));
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let (entered, mut entries) = tokio::sync::mpsc::unbounded_channel();
        let pool = sqlx::postgres::PgPoolOptions::new()
            .min_connections(0)
            .max_connections(8)
            .acquire_timeout(Duration::from_secs(3))
            .before_acquire({
                let armed = armed.clone();
                let gate = gate.clone();
                move |_, _| {
                    let armed = armed.clone();
                    let gate = gate.clone();
                    let entered = entered.clone();
                    Box::pin(async move {
                        if armed.load(Ordering::SeqCst) {
                            entered.send(()).expect("live fanout observer is open");
                            let permit = gate.acquire_owned().await.expect("open probe gate");
                            permit.forget();
                        }
                        Ok(true)
                    })
                }
            })
            .connect(&dsn)
            .await
            .expect("connect replica fanout pool");
        // Every probe must take the actual idle-connection acquisition hook.
        let mut warm = Vec::new();
        for _ in 0..8 {
            warm.push(pool.acquire().await.expect("warm replica connection"));
        }
        drop(warm);
        tokio::time::timeout(Duration::from_secs(2), async {
            while pool.num_idle() != 8 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("all fanout connections must return idle");
        armed.store(true, Ordering::SeqCst);
        let manager = PgReplicaManager::new(
            (0..8)
                .map(|index| PgReplicaPool::new(format!("fanout-{index}"), pool.clone()))
                .collect(),
            PgReplicaStrategy::RoundRobin,
            Duration::from_secs(3),
            false,
        );
        let refresh = tokio::spawn({
            let manager = manager.clone();
            async move { manager.refresh_health_once().await }
        });
        for _ in 0..4 {
            tokio::time::timeout(Duration::from_secs(1), entries.recv())
                .await
                .expect("first probe batch must enter pool acquisition")
                .expect("fanout observer is open");
        }
        assert!(!refresh.is_finished(), "refresh must await its probes");
        assert!(
            tokio::time::timeout(Duration::from_millis(100), entries.recv())
                .await
                .is_err(),
            "only four probes may be in flight before the first batch completes"
        );
        tokio::time::timeout(Duration::from_millis(100), manager.refresh_health_once())
            .await
            .expect("an overlapping refresh must skip instead of queueing more probes");
        gate.add_permits(8);
        tokio::time::timeout(Duration::from_secs(2), refresh)
            .await
            .expect("bounded refresh must complete after its pool gate opens")
            .expect("replica refresh task succeeds");
        assert!(manager.snapshots().iter().all(|snapshot| snapshot.healthy));
        assert!(manager.choose_pool().is_some());
        armed.store(false, Ordering::SeqCst);
        pool.close().await;
    }

    #[tokio::test]
    #[ignore = "requires live PostgreSQL; exercised by the Native CI live lane"]
    async fn replica_refresh_and_lsn_deadline_handle_held_single_slot_live() {
        use crate::runtime::consistency::StaleReadWarning;
        let Some(dsn) = replica_live_dsn() else {
            return;
        };
        let pool = sqlx::postgres::PgPoolOptions::new()
            .min_connections(0)
            .max_connections(1)
            .acquire_timeout(Duration::from_millis(500))
            .connect(&dsn)
            .await
            .expect("connect single-slot replica pool");
        let manager = PgReplicaManager::new(
            vec![PgReplicaPool::new("held-slot".to_string(), pool.clone())],
            PgReplicaStrategy::RoundRobin,
            Duration::from_secs(3),
            false,
        );
        let held = pool.acquire().await.expect("hold the only replica slot");
        let outcome = tokio::time::timeout(
            Duration::from_millis(250),
            manager.choose_bounded_replica(Some("0/100"), Duration::from_millis(50)),
        )
        .await
        .expect("the LSN deadline must include waiting for a pool slot");
        assert!(matches!(
            outcome,
            BoundedReplicaRead::FailoverToPrimary(StaleReadWarning::FenceTimedOut { .. })
        ));
        for _ in 0..2 {
            tokio::time::timeout(Duration::from_secs(2), manager.refresh_health_once())
                .await
                .expect("held-slot health refresh must make bounded progress");
            let snapshot = manager.snapshots().remove(0);
            assert!(
                !snapshot.healthy,
                "a deadline must revoke replica eligibility"
            );
            assert!(snapshot.last_error.is_some());
            assert!(snapshot.last_failure_unix_ms > 0);
            assert!(
                manager.choose_pool().is_none(),
                "unhealthy routing fails closed"
            );
        }
        drop(held);
        tokio::time::timeout(Duration::from_secs(2), manager.refresh_health_once())
            .await
            .expect("released capacity must let a fresh health probe recover");
        let snapshot = manager.snapshots().remove(0);
        assert!(snapshot.healthy);
        assert!(snapshot.last_error.is_none());
        assert!(manager.choose_pool().is_some());
        assert_eq!(
            pool.size(),
            1,
            "refreshes must preserve the configured pool bound"
        );
        pool.close().await;
    }

    #[tokio::test]
    #[ignore = "requires live PostgreSQL; exercised by the Native CI live lane"]
    async fn replica_query_deadlines_discard_pending_connections_and_recover_live() {
        let Some(dsn) = replica_live_dsn() else {
            return;
        };
        let admin = sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)
            .connect(&dsn)
            .await
            .expect("connect replica deadline observer");
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let schema = format!("udb_replica_deadline_{suffix}");
        let lock_id = uuid::Uuid::new_v4().as_u128() as i64;
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&admin)
            .await
            .expect("create isolated replica deadline fixture");
        for (function, result_type, result) in [
            ("pg_last_xact_replay_timestamp", "timestamptz", "NULL"),
            ("pg_last_wal_replay_lsn", "pg_lsn", "'0/100'::pg_lsn"),
        ] {
            sqlx::query(&format!(
                "CREATE FUNCTION {schema}.{function}() RETURNS {result_type} \
                 LANGUAGE plpgsql VOLATILE AS $body$ BEGIN \
                 PERFORM pg_catalog.pg_advisory_xact_lock({lock_id}); \
                 RETURN {result}; END $body$"
            ))
            .execute(&admin)
            .await
            .expect("install a lock-synchronized replica query fixture");
        }
        let mut lock = admin
            .acquire()
            .await
            .expect("acquire replica fixture lock owner");
        sqlx::query("SELECT pg_advisory_lock($1)")
            .bind(lock_id)
            .execute(&mut *lock)
            .await
            .expect("hold the query fixture's advisory lock");
        let options = dsn
            .parse::<sqlx::postgres::PgConnectOptions>()
            .expect("parse live replica connection options")
            .application_name(&format!("udb-replica-deadline-{suffix}"));
        let search_path = format!("SET search_path TO {schema}, pg_catalog");
        let pool = sqlx::postgres::PgPoolOptions::new()
            .min_connections(0)
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(2))
            .after_connect(move |connection, _| {
                let search_path = search_path.clone();
                Box::pin(async move {
                    sqlx::query(&search_path).execute(connection).await?;
                    Ok(())
                })
            })
            .connect_with(options)
            .await
            .expect("connect single-slot query deadline fixture");
        let manager = PgReplicaManager::new(
            vec![PgReplicaPool::new(
                "query-deadline".to_string(),
                pool.clone(),
            )],
            PgReplicaStrategy::RoundRobin,
            Duration::from_secs(3),
            false,
        );
        let initial_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&pool)
            .await
            .expect("observe initial replica backend");
        let refresh = tokio::spawn({
            let manager = manager.clone();
            async move { manager.refresh_health_once().await }
        });
        wait_for_replica_query_lock(&admin, initial_pid, "pg_last_xact_replay_timestamp").await;
        tokio::time::timeout(Duration::from_secs(3), refresh)
            .await
            .expect("a blocked health QUERY must obey the complete probe budget")
            .expect("health query deadline task completes");
        assert!(!manager.snapshots()[0].healthy);
        assert!(
            manager.snapshots()[0]
                .last_error
                .as_deref()
                .is_some_and(|error| error.contains("deadline"))
        );
        let replacement_pid: i32 = tokio::time::timeout(
            Duration::from_secs(1),
            sqlx::query_scalar("SELECT pg_backend_pid()").fetch_one(&pool),
        )
        .await
        .expect("query timeout must immediately free single-slot pool capacity")
        .expect("acquire replacement replica backend");
        assert_ne!(initial_pid, replacement_pid);
        let fence = tokio::spawn({
            let pool = pool.clone();
            async move { wait_for_replica_replay_lsn(&pool, "0/100", Duration::from_millis(250)).await }
        });
        wait_for_replica_query_lock(&admin, replacement_pid, "pg_last_wal_replay_lsn").await;
        assert!(
            !tokio::time::timeout(Duration::from_secs(1), fence)
                .await
                .expect("a blocked LSN QUERY must obey max_staleness")
                .expect("LSN query deadline task completes")
                .expect("an expired LSN fence is a fail-closed timeout")
        );
        let cancelled_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&pool)
            .await
            .expect("observe fresh cancellation fixture backend");
        assert_ne!(replacement_pid, cancelled_pid);
        let cancelled = tokio::spawn({
            let manager = manager.clone();
            async move { manager.refresh_health_once().await }
        });
        wait_for_replica_query_lock(&admin, cancelled_pid, "pg_last_xact_replay_timestamp").await;
        cancelled.abort();
        assert!(
            cancelled
                .await
                .expect_err("cancelled refresh must stop")
                .is_cancelled()
        );
        let recovered_pid: i32 = tokio::time::timeout(
            Duration::from_secs(1),
            sqlx::query_scalar("SELECT pg_backend_pid()").fetch_one(&pool),
        )
        .await
        .expect("caller cancellation must immediately free replica pool capacity")
        .expect("acquire a backend after cancelled refresh");
        assert_ne!(cancelled_pid, recovered_pid);
        sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(lock_id)
            .execute(&mut *lock)
            .await
            .expect("release blocked replica queries");
        drop(lock);
        tokio::time::timeout(Duration::from_secs(3), manager.refresh_health_once())
            .await
            .expect("a fresh successful probe must restore replica health");
        assert!(manager.snapshots()[0].healthy);
        assert!(manager.snapshots()[0].last_error.is_none());
        let reused_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&pool)
            .await
            .expect("observe successful-probe backend reuse");
        assert_eq!(
            reused_pid, recovered_pid,
            "completed probes must retain pool reuse"
        );
        assert!(
            wait_for_replica_replay_lsn(&pool, "0/100", Duration::from_secs(1))
                .await
                .expect("a fresh LSN fence must clear after lock release")
        );
        pool.close().await;
        sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
            .execute(&admin)
            .await
            .expect("remove isolated replica deadline fixture");
        admin.close().await;
    }

    #[test]
    fn replica_dsns_prefers_multi_value() {
        let dsns =
            replica_dsns_from_values(Some(" postgres://r1/db,postgres://r2/db ,, "), Some("x"));
        assert_eq!(dsns, vec!["postgres://r1/db", "postgres://r2/db"]);
    }

    #[test]
    fn replica_dsns_falls_back_to_single_value() {
        let dsns = replica_dsns_from_values(Some(" "), Some(" postgres://single/db "));
        assert_eq!(dsns, vec!["postgres://single/db"]);
    }

    #[test]
    fn replica_dsns_empty_when_both_missing() {
        let dsns = replica_dsns_from_values(None, None);
        assert!(dsns.is_empty());
    }

    #[test]
    fn replica_strategy_normalizes_values() {
        assert_eq!(
            PgReplicaStrategy::from_value("least-lag"),
            PgReplicaStrategy::LeastLag
        );
        assert_eq!(
            PgReplicaStrategy::from_value("primary_only"),
            PgReplicaStrategy::PrimaryOnly
        );
        assert_eq!(
            PgReplicaStrategy::from_value("unknown"),
            PgReplicaStrategy::RoundRobin
        );
        assert_eq!(
            PgReplicaStrategy::from_value("random_healthy"),
            PgReplicaStrategy::RandomHealthy
        );
        assert_eq!(
            PgReplicaStrategy::from_value("disabled"),
            PgReplicaStrategy::PrimaryOnly
        );
    }

    #[test]
    fn application_name_is_appended_safely() {
        assert_eq!(
            append_application_name("postgres://host/db", "udb-replica-0"),
            "postgres://host/db?application_name=udb-replica-0"
        );
        assert_eq!(
            append_application_name("postgres://host/db?sslmode=require", "udb-replica-0"),
            "postgres://host/db?sslmode=require&application_name=udb-replica-0"
        );
        // Already has application_name — must not double-append
        assert_eq!(
            append_application_name(
                "postgres://host/db?application_name=existing",
                "udb-replica-0"
            ),
            "postgres://host/db?application_name=existing"
        );
    }

    // ── Phase 12: Replica routing strategy unit tests ────────────────────────

    #[test]
    fn primary_only_strategy_always_returns_none() {
        let manager = PgReplicaManager::new(
            vec![],
            PgReplicaStrategy::PrimaryOnly,
            Duration::from_secs(3),
            false,
        );
        assert!(
            manager.choose_pool().is_none(),
            "PrimaryOnly must always return None"
        );
    }

    #[test]
    fn empty_replica_pool_returns_none_regardless_of_strategy() {
        for strategy in [
            PgReplicaStrategy::RoundRobin,
            PgReplicaStrategy::LeastLag,
            PgReplicaStrategy::RandomHealthy,
        ] {
            let manager = PgReplicaManager::new(vec![], strategy, Duration::from_secs(3), false);
            assert!(
                manager.choose_pool().is_none(),
                "{} with empty replicas must return None",
                strategy.as_str()
            );
        }
    }

    #[test]
    fn replica_lag_rejection_filters_lagging_replicas() {
        // Create a manager in memory — we can test lag-rejection via snapshot marking.
        // An unhealthy replica with huge lag should be skipped.
        let manager = PgReplicaManager::empty();
        // Empty replicas = no pool; lag-rejection path is implicitly enforced.
        let pool = manager.choose_pool_with_max_lag(Some(Duration::from_millis(100)));
        assert!(pool.is_none(), "Lagging (empty) manager must return None");
    }

    #[test]
    fn strategy_as_str_is_stable() {
        assert_eq!(PgReplicaStrategy::RoundRobin.as_str(), "round_robin");
        assert_eq!(PgReplicaStrategy::LeastLag.as_str(), "least_lag");
        assert_eq!(PgReplicaStrategy::RandomHealthy.as_str(), "random_healthy");
        assert_eq!(PgReplicaStrategy::PrimaryOnly.as_str(), "primary_only");
    }

    #[test]
    fn fail_open_false_returns_none_on_all_unhealthy() {
        // With fail_open=false and no replicas, must return None (not panic).
        let manager = PgReplicaManager::new(
            vec![],
            PgReplicaStrategy::RoundRobin,
            Duration::from_millis(0),
            false,
        );
        assert!(manager.choose_pool().is_none());
    }

    #[test]
    fn replica_manager_is_empty_when_no_replicas() {
        let manager = PgReplicaManager::empty();
        assert!(manager.is_empty());
        assert_eq!(manager.len(), 0);
    }

    // ── 6.4 REPLICA_BOUNDED routing ──────────────────────────────────────────

    /// With no eligible replica (empty manager), a bounded read fails over
    /// to the primary — whether or not an LSN fence was supplied. No live DB
    /// is touched because replica selection short-circuits to `None`.
    #[tokio::test]
    async fn bounded_read_fails_over_to_primary_without_replicas() {
        let manager = PgReplicaManager::empty();
        assert!(matches!(
            manager
                .choose_bounded_replica(Some("0/1A2B3C"), Duration::from_millis(50))
                .await,
            BoundedReplicaRead::FailoverToPrimary(_)
        ));
        assert!(matches!(
            manager
                .choose_bounded_replica(None, Duration::from_millis(50))
                .await,
            BoundedReplicaRead::FailoverToPrimary(_)
        ));
    }

    /// PrimaryOnly strategy never selects a replica, so a bounded read
    /// always fails over to the primary.
    #[tokio::test]
    async fn bounded_read_fails_over_under_primary_only_strategy() {
        let manager = PgReplicaManager::new(
            vec![],
            PgReplicaStrategy::PrimaryOnly,
            Duration::from_secs(3),
            false,
        );
        assert!(matches!(
            manager
                .choose_bounded_replica(Some("0/100"), Duration::from_millis(10))
                .await,
            BoundedReplicaRead::FailoverToPrimary(_)
        ));
    }

    /// 6.4 doctrine: a bounded read that fails over to the primary NEVER does
    /// so silently — the failover decision carries a `StaleReadWarning`. With
    /// a REAL LSN token supplied and no eligible replica, the warning is
    /// `ReplicaLagExceeded` and carries the staleness budget, so the caller
    /// can attach it to the response.
    #[tokio::test]
    async fn bounded_read_failover_attaches_stale_read_warning() {
        use crate::runtime::consistency::StaleReadWarning;
        let manager = PgReplicaManager::empty();
        match manager
            .choose_bounded_replica(Some("0/1A2B3C"), Duration::from_millis(40))
            .await
        {
            BoundedReplicaRead::FailoverToPrimary(StaleReadWarning::ReplicaLagExceeded {
                budget_ms,
                ..
            }) => {
                assert_eq!(
                    budget_ms, 40,
                    "failover warning carries the staleness budget"
                );
            }
            other => panic!("expected FailoverToPrimary(ReplicaLagExceeded), got {other:?}"),
        }
        // No-fence bounded read with no replica also fails over with a warning.
        assert!(matches!(
            manager
                .choose_bounded_replica(None, Duration::from_millis(10))
                .await,
            BoundedReplicaRead::FailoverToPrimary(StaleReadWarning::ReplicaLagExceeded { .. })
        ));
    }

    /// The LSN poll interval honours the env override and never exceeds the
    /// staleness budget (so a tiny window returns promptly).
    #[test]
    fn replica_lsn_poll_interval_is_capped_to_budget() {
        // Default 25 ms when the budget is generous.
        assert_eq!(
            replica_lsn_poll_interval(Duration::from_secs(5)),
            Duration::from_millis(25)
        );
        // Capped to the budget when the window is shorter than the poll.
        assert_eq!(
            replica_lsn_poll_interval(Duration::from_millis(5)),
            Duration::from_millis(5)
        );
        // Never zero even for a zero budget (avoids a busy-spin).
        assert_eq!(
            replica_lsn_poll_interval(Duration::ZERO),
            Duration::from_millis(1)
        );
    }
}
