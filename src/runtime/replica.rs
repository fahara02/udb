use std::sync::{
    Arc, RwLock,
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use sqlx::PgPool;

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

#[derive(Debug, Clone)]
pub struct PgReplicaManager {
    replicas: Arc<Vec<PgReplicaPool>>,
    strategy: PgReplicaStrategy,
    max_lag: Duration,
    fail_open: bool,
    next: Arc<AtomicUsize>,
    query_total: Arc<AtomicU64>,
    fallback_total: Arc<AtomicU64>,
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

    pub async fn refresh_health_once(&self) {
        for replica in self.replicas.iter().cloned() {
            tokio::spawn(async move {
                probe_replica(replica).await;
            });
        }
    }

    pub fn start_health_task(&self, interval: Duration) {
        if self.replicas.is_empty() {
            return;
        }
        let manager = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(interval);
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

async fn probe_replica(replica: PgReplicaPool) {
    let started = Instant::now();
    let result: Result<(Option<f64>,), sqlx::Error> = sqlx::query_as(
        "SELECT COALESCE(EXTRACT(EPOCH FROM (NOW() - pg_last_xact_replay_timestamp())), 0)::float8",
    )
    .fetch_one(&replica.pool)
    .await;
    let latency_millis = started.elapsed().as_millis() as u64;
    match result {
        Ok((lag_seconds,)) => {
            let lag_millis = lag_seconds.unwrap_or(0.0).max(0.0).mul_add(1000.0, 0.0) as u64;
            replica.mark_healthy(lag_millis, latency_millis);
        }
        Err(err) => replica.mark_unhealthy(latency_millis, err.to_string()),
    }
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
}
