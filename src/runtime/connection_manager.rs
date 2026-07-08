use std::collections::HashMap;
use std::env;
use std::fmt;
use std::ops::Deref;
use std::sync::{
    Arc, Mutex, RwLock,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::Serialize;
use sqlx::PgPool;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tonic::Status;

use crate::runtime::channels::{
    SCOPE_MAP_IDLE_TTL, SCOPE_MAP_MAX_ENTRIES, ScopedSemaphoreEntry, env_part, evict_scope_map,
    scoped_value,
};
use crate::runtime::metrics::MetricsRecorder;

#[cfg(feature = "clickhouse")]
use crate::runtime::executors::clickhouse::ClickHouseExecutor;
#[cfg(feature = "mongodb")]
use crate::runtime::executors::mongodb::MongoDbExecutor;
#[cfg(feature = "neo4j")]
use crate::runtime::executors::neo4j::Neo4jExecutor;
#[cfg(feature = "qdrant")]
use crate::runtime::executors::qdrant::QdrantHttpClient;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum ClientState {
    Warming,
    Ready,
    Draining,
    Closed,
    Unhealthy,
}

impl ClientState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Warming => "warming",
            Self::Ready => "ready",
            Self::Draining => "draining",
            Self::Closed => "closed",
            Self::Unhealthy => "unhealthy",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum PoolingMode {
    Session,
    Transaction,
    Statement,
}

impl PoolingMode {
    pub fn from_labels(labels: &HashMap<String, String>) -> Self {
        labels
            .get("pooling_mode")
            .or_else(|| labels.get("pooling"))
            .or_else(|| labels.get("pool_mode"))
            .map(|value| Self::from_value(value))
            .unwrap_or(Self::Session)
    }

    pub fn from_value(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "transaction" | "transaction_pool" | "transaction_pooling" => Self::Transaction,
            "statement" | "statement_pool" | "statement_pooling" | "pipelined" => Self::Statement,
            _ => Self::Session,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Session => "session",
            Self::Transaction => "transaction",
            Self::Statement => "statement",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct ClientKey {
    pub project_id: String,
    pub backend: String,
    pub instance: String,
    pub role: String,
}

impl ClientKey {
    pub fn new(project_id: &str, backend: &str, instance: &str, role: &str) -> Self {
        Self {
            project_id: normalize_part(project_id, "default"),
            backend: normalize_part(backend, "unknown"),
            instance: normalize_part(instance, "default"),
            role: normalize_part(role, "read_write"),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ClientSnapshot {
    pub project_id: String,
    pub backend: String,
    pub instance: String,
    pub role: String,
    pub state: String,
    pub pooling_mode: String,
    pub created_at_unix_ms: u64,
    pub last_health_probe_unix_ms: u64,
    pub active_leases: u64,
    pub total_leases: u64,
    pub error_count: u64,
    pub last_error: Option<String>,
    pub lease_budget: Option<u64>,
    pub labels: HashMap<String, String>,
    pub pg_active_connections: Option<u32>,
    pub pg_idle_connections: Option<u32>,
}

#[derive(Clone, Debug)]
pub struct ClientLease<T> {
    value: T,
    active: Arc<AtomicU64>,
}

impl<T> ClientLease<T> {
    fn new(value: T, active: Arc<AtomicU64>, total: Arc<AtomicU64>) -> Self {
        active.fetch_add(1, Ordering::Relaxed);
        total.fetch_add(1, Ordering::Relaxed);
        Self { value, active }
    }

    pub fn into_inner(self) -> T
    where
        T: Clone,
    {
        self.value.clone()
    }
}

impl<T> Deref for ClientLease<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<T> Drop for ClientLease<T> {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::Relaxed);
    }
}

#[derive(Clone)]
enum ManagedClientKind {
    Postgres(PgPool),
    #[cfg(feature = "redis")]
    Redis(redis::Client),
    #[cfg(feature = "qdrant")]
    Qdrant(QdrantHttpClient),
    #[cfg(feature = "s3")]
    S3(aws_sdk_s3::Client),
    #[cfg(feature = "mongodb")]
    Mongodb(MongoDbExecutor),
    #[cfg(feature = "neo4j")]
    Neo4j(Neo4jExecutor),
    #[cfg(feature = "clickhouse")]
    ClickHouse(ClickHouseExecutor),
}

#[derive(Clone)]
struct ManagedClientEntry {
    key: ClientKey,
    kind: ManagedClientKind,
    state: ClientState,
    pooling_mode: PoolingMode,
    labels: HashMap<String, String>,
    created_at_unix_ms: u64,
    last_health_probe_unix_ms: Arc<AtomicU64>,
    active_leases: Arc<AtomicU64>,
    total_leases: Arc<AtomicU64>,
    error_count: Arc<AtomicU64>,
    last_error: Arc<Mutex<Option<String>>>,
    lease_budget: Option<u64>,
}

impl ManagedClientEntry {
    fn new(
        key: ClientKey,
        kind: ManagedClientKind,
        role: &str,
        labels: HashMap<String, String>,
    ) -> Self {
        let pooling_mode = PoolingMode::from_labels(&labels);
        let key = ClientKey {
            role: normalize_part(role, "read_write"),
            ..key
        };
        Self {
            key,
            kind,
            state: ClientState::Ready,
            pooling_mode,
            lease_budget: lease_budget_from_labels(&labels),
            labels,
            created_at_unix_ms: unix_millis(),
            last_health_probe_unix_ms: Arc::new(AtomicU64::new(0)),
            active_leases: Arc::new(AtomicU64::new(0)),
            total_leases: Arc::new(AtomicU64::new(0)),
            error_count: Arc::new(AtomicU64::new(0)),
            last_error: Arc::new(Mutex::new(None)),
        }
    }

    fn snapshot(&self) -> ClientSnapshot {
        let _kind = self.kind_label();
        let (pg_active_connections, pg_idle_connections) = match &self.kind {
            ManagedClientKind::Postgres(pool) => {
                let size = pool.size();
                let idle = pool.num_idle() as u32;
                (Some(size.saturating_sub(idle)), Some(idle))
            }
            #[cfg(any(
                feature = "redis",
                feature = "qdrant",
                feature = "s3",
                feature = "mongodb",
                feature = "neo4j",
                feature = "clickhouse"
            ))]
            _ => (None, None),
        };
        ClientSnapshot {
            project_id: self.key.project_id.clone(),
            backend: self.key.backend.clone(),
            instance: self.key.instance.clone(),
            role: self.key.role.clone(),
            state: self.state.as_str().to_string(),
            pooling_mode: self.pooling_mode.as_str().to_string(),
            created_at_unix_ms: self.created_at_unix_ms,
            last_health_probe_unix_ms: self.last_health_probe_unix_ms.load(Ordering::Relaxed),
            active_leases: self.active_leases.load(Ordering::Relaxed),
            total_leases: self.total_leases.load(Ordering::Relaxed),
            error_count: self.error_count.load(Ordering::Relaxed),
            last_error: self.last_error.lock().ok().and_then(|guard| guard.clone()),
            lease_budget: self.lease_budget,
            labels: self.labels.clone(),
            pg_active_connections,
            pg_idle_connections,
        }
    }

    fn kind_label(&self) -> &'static str {
        match &self.kind {
            ManagedClientKind::Postgres(pool) => {
                let _ = pool;
                "postgres"
            }
            #[cfg(feature = "redis")]
            ManagedClientKind::Redis(client) => {
                let _ = client;
                "redis"
            }
            #[cfg(feature = "qdrant")]
            ManagedClientKind::Qdrant(client) => {
                let _ = client;
                "qdrant"
            }
            #[cfg(feature = "s3")]
            ManagedClientKind::S3(client) => {
                let _ = client;
                "s3"
            }
            #[cfg(feature = "mongodb")]
            ManagedClientKind::Mongodb(executor) => {
                let _ = executor;
                "mongodb"
            }
            #[cfg(feature = "neo4j")]
            ManagedClientKind::Neo4j(executor) => {
                let _ = executor;
                "neo4j"
            }
            #[cfg(feature = "clickhouse")]
            ManagedClientKind::ClickHouse(executor) => {
                let _ = executor;
                "clickhouse"
            }
        }
    }
}

/// Default bounded deadline (ms) a tenant at its DB-connection budget will QUEUE
/// for a slot before the broker returns `ResourceExhausted` rather than draining
/// the shared pool. Mirrors the channels.rs read/write queue-timeout default
/// (`OperationChannel::default_queue_timeout_ms` = 250ms).
const TENANT_CONN_QUEUE_TIMEOUT_MS: u64 = 250;

/// Per-tenant DB-connection budget configuration, resolved ONCE from the
/// environment at `ConnectionManager` construction (never per-request). This
/// extends the channels.rs admission fairness down to the actual driver/pool
/// layer (PgBouncer-class per-tenant connection caps).
///
/// Like channels.rs `ChannelEnvConfig`, the env is read once into a name-keyed
/// map so per-tenant lookups reconstruct the var name from the tenant id and
/// need neither a reverse name mapping nor any per-request env read.
#[derive(Debug, Clone, Default)]
struct TenantBudgetConfig {
    /// `UDB_TENANT_<TENANT>_CONN_BUDGET` overrides, keyed by the fully-expanded
    /// env var name. Resolved once at construction.
    values: HashMap<String, usize>,
    /// `UDB_TENANT_CONN_BUDGET_DEFAULT` — the per-tenant budget applied when no
    /// tenant-specific override exists. `None` = no per-tenant cap (preserves the
    /// pre-tiering behavior: a tenant can use the whole shared pool).
    default_budget: Option<usize>,
    /// `UDB_TENANT_CONN_QUEUE_TIMEOUT_MS` — bounded queue deadline for a tenant
    /// at budget.
    queue_timeout_ms: u64,
}

impl TenantBudgetConfig {
    fn from_env() -> Self {
        Self::from_pairs(env::vars())
    }

    fn from_pairs<I, K, V>(pairs: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        let raw: HashMap<String, String> = pairs
            .into_iter()
            .map(|(key, value)| (key.into(), value.into()))
            .collect();
        let values = raw
            .iter()
            .filter(|(key, _)| key.starts_with("UDB_TENANT_") && key.ends_with("_CONN_BUDGET"))
            .filter_map(|(key, value)| {
                value
                    .trim()
                    .parse::<usize>()
                    .ok()
                    .map(|parsed| (key.clone(), parsed))
            })
            .collect();
        let default_budget = raw
            .get("UDB_TENANT_CONN_BUDGET_DEFAULT")
            .and_then(|value| value.trim().parse::<usize>().ok())
            .filter(|budget| *budget > 0);
        let queue_timeout_ms = raw
            .get("UDB_TENANT_CONN_QUEUE_TIMEOUT_MS")
            .and_then(|value| value.trim().parse::<u64>().ok())
            .unwrap_or(TENANT_CONN_QUEUE_TIMEOUT_MS);
        Self {
            values,
            default_budget,
            queue_timeout_ms,
        }
    }

    /// Resolve a tenant's connection budget: tenant-specific override → default →
    /// `None` (unlimited). Pure map lookup; no env read.
    fn budget_for(&self, tenant: &str) -> Option<usize> {
        let key = format!("UDB_TENANT_{}_CONN_BUDGET", env_part(tenant));
        self.values
            .get(&key)
            .copied()
            .or(self.default_budget)
            .filter(|budget| *budget > 0)
    }
}

/// RAII guard for a per-tenant DB-connection budget slot. Dropping it returns the
/// slot to the tenant's semaphore so a queued admission can proceed.
#[derive(Debug)]
pub struct TenantConnectionPermit {
    _permit: OwnedSemaphorePermit,
}

/// A budget-enforced Postgres pool lease: the [`ClientLease`] accounting handle
/// plus the per-tenant DB-connection budget permit (`None` when the tenant is
/// unbudgeted). Hold it for the lifetime of the borrowed connection — dropping
/// it releases BOTH the lease accounting and the tenant's budget slot.
#[derive(Debug)]
pub struct TenantPgLease {
    lease: ClientLease<PgPool>,
    _permit: Option<TenantConnectionPermit>,
}

impl TenantPgLease {
    /// The leased pool to run the op against.
    pub fn pool(&self) -> PgPool {
        (*self.lease).clone()
    }
}

impl Deref for TenantPgLease {
    type Target = PgPool;

    fn deref(&self) -> &PgPool {
        &self.lease
    }
}

#[derive(Clone)]
pub struct ConnectionManager {
    inner: Arc<RwLock<HashMap<ClientKey, ManagedClientEntry>>>,
    /// Per-tenant connection budgets, resolved once at construction.
    budgets: Arc<TenantBudgetConfig>,
    /// Per-tenant budget semaphores. REUSES the channels.rs scope-semaphore value
    /// type [`ScopedSemaphoreEntry`] (sem + limit + idle-TTL `last_access`) and its
    /// `evict_scope_map` bounding helper — this is the channels.rs `ScopeSemMap`
    /// pattern, keyed on the tenant id rather than a precomputed hash since this
    /// admission path is off the per-request channels hot loop.
    tenant_slots: Arc<Mutex<HashMap<String, ScopedSemaphoreEntry>>>,
    /// Optional process metrics recorder, used only to surface the per-tenant
    /// starvation gauge. `None` (default) keeps the gauge silent.
    metrics: Option<Arc<dyn MetricsRecorder + Send + Sync>>,
}

impl Default for ConnectionManager {
    fn default() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
            // Resolve-ONCE: per-tenant budgets are read from the environment a
            // single time here, at manager construction — never per request.
            budgets: Arc::new(TenantBudgetConfig::from_env()),
            tenant_slots: Arc::new(Mutex::new(HashMap::new())),
            metrics: None,
        }
    }
}

impl fmt::Debug for ConnectionManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let len = self
            .inner
            .read()
            .map(|guard| guard.len())
            .unwrap_or_default();
        f.debug_struct("ConnectionManager")
            .field("clients", &len)
            .finish()
    }
}

impl ConnectionManager {
    pub(crate) fn register_postgres(
        &self,
        instance: &str,
        role: &str,
        pool: PgPool,
        labels: HashMap<String, String>,
    ) {
        self.register(
            ClientKey::new("default", "postgres", instance, role),
            ManagedClientKind::Postgres(pool),
            role,
            labels,
        );
    }

    #[cfg(feature = "redis")]
    pub(crate) fn register_redis(
        &self,
        instance: &str,
        role: &str,
        client: redis::Client,
        labels: HashMap<String, String>,
    ) {
        self.register(
            ClientKey::new("default", "redis", instance, role),
            ManagedClientKind::Redis(client),
            role,
            labels,
        );
    }

    #[cfg(feature = "qdrant")]
    pub(crate) fn register_qdrant(
        &self,
        instance: &str,
        role: &str,
        client: QdrantHttpClient,
        labels: HashMap<String, String>,
    ) {
        self.register(
            ClientKey::new("default", "qdrant", instance, role),
            ManagedClientKind::Qdrant(client),
            role,
            labels,
        );
    }

    #[cfg(feature = "s3")]
    pub(crate) fn register_s3(
        &self,
        backend: &str,
        instance: &str,
        role: &str,
        client: aws_sdk_s3::Client,
        labels: HashMap<String, String>,
    ) {
        self.register(
            ClientKey::new("default", backend, instance, role),
            ManagedClientKind::S3(client),
            role,
            labels,
        );
    }

    #[cfg(feature = "mongodb")]
    pub(crate) fn register_mongodb(
        &self,
        instance: &str,
        role: &str,
        executor: MongoDbExecutor,
        labels: HashMap<String, String>,
    ) {
        self.register(
            ClientKey::new("default", "mongodb", instance, role),
            ManagedClientKind::Mongodb(executor),
            role,
            labels,
        );
    }

    #[cfg(feature = "neo4j")]
    pub(crate) fn register_neo4j(
        &self,
        instance: &str,
        role: &str,
        executor: Neo4jExecutor,
        labels: HashMap<String, String>,
    ) {
        self.register(
            ClientKey::new("default", "neo4j", instance, role),
            ManagedClientKind::Neo4j(executor),
            role,
            labels,
        );
    }

    #[cfg(feature = "clickhouse")]
    pub(crate) fn register_clickhouse(
        &self,
        instance: &str,
        role: &str,
        executor: ClickHouseExecutor,
        labels: HashMap<String, String>,
    ) {
        self.register(
            ClientKey::new("default", "clickhouse", instance, role),
            ManagedClientKind::ClickHouse(executor),
            role,
            labels,
        );
    }

    fn register(
        &self,
        key: ClientKey,
        kind: ManagedClientKind,
        role: &str,
        labels: HashMap<String, String>,
    ) {
        if let Ok(mut guard) = self.inner.write() {
            guard.insert(
                key.clone(),
                ManagedClientEntry::new(key, kind, role, labels),
            );
        }
    }

    /// Attach the process metrics recorder so per-tenant budget starvation is
    /// observable (`udb_connection_tenant_budget_starved`). Wired once at startup;
    /// `None` (the default) keeps the gauge silent.
    pub fn with_metrics(mut self, metrics: Arc<dyn MetricsRecorder + Send + Sync>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Admit a tenant to lease a DB connection under its per-tenant budget,
    /// extending the channels.rs admission fairness down to the driver/pool layer
    /// (PgBouncer-class per-tenant connection caps).
    ///
    /// A tenant at its budget QUEUES on a bounded deadline (mirroring
    /// `channels.rs::acquire_one`) instead of draining the shared pool, so one
    /// tenant's connection pressure cannot starve other tenants. Returns:
    /// - `Ok(None)` when no budget is configured for the tenant (unlimited — the
    ///   pre-tiering behavior),
    /// - `Ok(Some(permit))` when admitted; hold the permit for the lifetime of the
    ///   borrowed connection so dropping it frees the slot,
    /// - `Err(ResourceExhausted)` when the tenant's budget stays full past the
    ///   queue deadline.
    pub async fn acquire_tenant_connection(
        &self,
        tenant: Option<&str>,
    ) -> Result<Option<TenantConnectionPermit>, Status> {
        let Some(tenant) = scoped_value(tenant) else {
            return Ok(None);
        };
        let Some(budget) = self.budgets.budget_for(tenant) else {
            return Ok(None);
        };
        let sem = self.tenant_slot(tenant, budget);
        // Fast path: a free slot, admit without queueing or touching the gauge.
        if let Ok(permit) = sem.clone().try_acquire_owned() {
            return Ok(Some(TenantConnectionPermit { _permit: permit }));
        }
        // At budget — QUEUE on the bounded deadline. Mirrors channels.rs
        // `acquire_one`: a zero timeout rejects immediately, otherwise wait up to
        // the deadline for a slot to free.
        self.set_starved(tenant, true);
        let timeout = self.budgets.queue_timeout_ms;
        let outcome = if timeout == 0 {
            Err(())
        } else {
            match tokio::time::timeout(Duration::from_millis(timeout), sem.acquire_owned()).await {
                Ok(Ok(permit)) => Ok(permit),
                // Closed semaphore or deadline elapsed: treat both as exhaustion.
                Ok(Err(_)) | Err(_) => Err(()),
            }
        };
        self.set_starved(tenant, false);
        match outcome {
            Ok(permit) => Ok(Some(TenantConnectionPermit { _permit: permit })),
            Err(()) => {
                let retry_after_ms = timeout.max(1) as i64;
                let message = format!(
                    "tenant '{tenant}' DB-connection budget ({budget}) exhausted; retry after {retry_after_ms}ms"
                );
                Err(crate::runtime::executor_utils::quota_status(
                    "connection_manager",
                    "tenant_connection_budget",
                    retry_after_ms,
                    message,
                ))
            }
        }
    }

    /// Resolve-or-create a tenant's budget semaphore. The limit is taken ONCE from
    /// the constructor-time budget config when the entry is first created and is
    /// never re-resolved. Bounds the map by REUSING channels.rs `evict_scope_map`
    /// (same call shape as `scoped_sem` `#206`): once at cap, evict fully-returned
    /// (idle, past idle-TTL) tenants before inserting a new one.
    fn tenant_slot(&self, tenant: &str, budget: usize) -> Arc<Semaphore> {
        let mut guard = self
            .tenant_slots
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !guard.contains_key(tenant) {
            evict_scope_map(
                &mut guard,
                SCOPE_MAP_MAX_ENTRIES.saturating_sub(1),
                SCOPE_MAP_IDLE_TTL,
                |entry| entry.last_access,
                |entry| !entry.has_outstanding_permits(),
            );
        }
        let now = Instant::now();
        let entry = guard
            .entry(tenant.to_string())
            .or_insert_with(|| ScopedSemaphoreEntry {
                sem: Arc::new(Semaphore::new(budget)),
                last_access: now,
                limit: budget,
            });
        entry.last_access = now;
        Arc::clone(&entry.sem)
    }

    fn set_starved(&self, tenant: &str, starved: bool) {
        if let Some(metrics) = &self.metrics {
            metrics.set_tenant_connection_starved(tenant, starved);
        }
    }

    pub(crate) fn lease_postgres(&self, instance: &str) -> Option<ClientLease<PgPool>> {
        let entry = self.entry("postgres", instance)?;
        if !entry.lease_budget_allows() {
            return None;
        }
        match entry.kind {
            ManagedClientKind::Postgres(pool) => Some(ClientLease::new(
                pool,
                entry.active_leases,
                entry.total_leases,
            )),
            #[cfg(any(
                feature = "redis",
                feature = "qdrant",
                feature = "s3",
                feature = "mongodb",
                feature = "neo4j",
                feature = "clickhouse"
            ))]
            _ => None,
        }
    }

    /// Lease a Postgres pool for a tenant-scoped op, enforcing the tenant's
    /// per-tenant DB-connection budget FIRST and tying the budget permit to the
    /// returned lease's lifetime.
    ///
    /// This is the budget-aware hand-out wrapper around [`Self::lease_postgres`]:
    /// it calls [`Self::acquire_tenant_connection`] before the pooled connection
    /// is leased, so a tenant at its budget QUEUES (and ultimately gets
    /// `ResourceExhausted`) instead of draining the shared pool. The returned
    /// [`TenantPgLease`] holds both the lease accounting and the budget permit;
    /// dropping it frees the tenant's slot. Returns:
    /// - `Err(ResourceExhausted)` when the tenant stays at budget past the queue
    ///   deadline,
    /// - `Ok(None)` when the instance pool is not registered/ready (mirrors
    ///   `lease_postgres`),
    /// - `Ok(Some(lease))` otherwise. An unbudgeted tenant (`acquire_tenant_connection`
    ///   returns `Ok(None)`) leases with no permit — the pre-tiering unlimited
    ///   behavior.
    pub async fn lease_postgres_for_tenant(
        &self,
        instance: &str,
        tenant: Option<&str>,
    ) -> Result<Option<TenantPgLease>, Status> {
        let permit = self.acquire_tenant_connection(tenant).await?;
        Ok(self.lease_postgres(instance).map(|lease| TenantPgLease {
            lease,
            _permit: permit,
        }))
    }

    pub fn mark_state(&self, backend: &str, instance: &str, state: ClientState) -> bool {
        let Some(key) = self.find_key(backend, instance) else {
            return false;
        };
        self.inner
            .write()
            .ok()
            .and_then(|mut guard| {
                guard.get_mut(&key).map(|entry| {
                    entry.state = state;
                })
            })
            .is_some()
    }

    pub fn mark_draining(&self, backend: &str, instance: &str) -> bool {
        self.mark_state(backend, instance, ClientState::Draining)
    }

    pub fn mark_closed(&self, backend: &str, instance: &str) -> bool {
        self.mark_state(backend, instance, ClientState::Closed)
    }

    pub fn record_health_result(
        &self,
        backend: &str,
        instance: &str,
        ok: bool,
        error: Option<String>,
    ) -> bool {
        let Some(key) = self.find_key(backend, instance) else {
            return false;
        };
        self.inner
            .write()
            .ok()
            .and_then(|mut guard| {
                guard.get_mut(&key).map(|entry| {
                    entry
                        .last_health_probe_unix_ms
                        .store(unix_millis(), Ordering::Relaxed);
                    if ok {
                        entry.state = ClientState::Ready;
                        if let Ok(mut last_error) = entry.last_error.lock() {
                            *last_error = None;
                        }
                    } else {
                        entry.state = ClientState::Unhealthy;
                        entry.error_count.fetch_add(1, Ordering::Relaxed);
                        if let Ok(mut last_error) = entry.last_error.lock() {
                            *last_error = error;
                        }
                    }
                })
            })
            .is_some()
    }

    pub fn replace_all_from(&self, shadow: &ConnectionManager) {
        let replacement = shadow.inner.read().map(|guard| guard.clone());
        if let (Ok(replacement), Ok(mut current)) = (replacement, self.inner.write()) {
            let mut next = replacement;
            for entry in current.values_mut() {
                if entry.active_leases.load(Ordering::Relaxed) > 0 {
                    entry.state = ClientState::Draining;
                    let mut retained = entry.clone();
                    retained.key.instance = format!(
                        "{}__draining_{}",
                        retained.key.instance, retained.created_at_unix_ms
                    );
                    retained.key.role = format!("{}_draining", retained.key.role);
                    next.insert(retained.key.clone(), retained);
                } else {
                    entry.state = ClientState::Closed;
                }
            }
            *current = next;
        }
    }

    pub fn active_leases(&self, backend: &str, instance: &str) -> Option<u64> {
        self.entry_any_state(backend, instance)
            .map(|entry| entry.active_leases.load(Ordering::Relaxed))
    }

    pub fn snapshots(&self) -> Vec<ClientSnapshot> {
        let mut snapshots = self
            .inner
            .read()
            .map(|guard| {
                guard
                    .values()
                    .map(ManagedClientEntry::snapshot)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        snapshots.sort_by(|a, b| {
            (
                a.project_id.as_str(),
                a.backend.as_str(),
                a.instance.as_str(),
                a.role.as_str(),
            )
                .cmp(&(
                    b.project_id.as_str(),
                    b.backend.as_str(),
                    b.instance.as_str(),
                    b.role.as_str(),
                ))
        });
        snapshots
    }

    pub fn metrics_text(&self) -> String {
        let snapshots = self.snapshots();
        if snapshots.is_empty() {
            return String::new();
        }
        let mut out = String::new();
        out.push_str("# TYPE udb_connection_client_state gauge\n");
        out.push_str("# TYPE udb_connection_client_active_leases gauge\n");
        out.push_str("# TYPE udb_connection_client_total_leases counter\n");
        out.push_str("# TYPE udb_connection_client_errors_total counter\n");
        out.push_str("# TYPE udb_connection_client_lease_budget gauge\n");
        out.push_str("# TYPE udb_connection_client_last_health_probe_unix_ms gauge\n");
        out.push_str("# TYPE udb_connection_pg_active_connections gauge\n");
        out.push_str("# TYPE udb_connection_pg_idle_connections gauge\n");
        for snapshot in snapshots {
            let labels = format!(
                "project=\"{}\",backend=\"{}\",instance=\"{}\",role=\"{}\",state=\"{}\",pooling_mode=\"{}\"",
                metric_escape(&snapshot.project_id),
                metric_escape(&snapshot.backend),
                metric_escape(&snapshot.instance),
                metric_escape(&snapshot.role),
                metric_escape(&snapshot.state),
                metric_escape(&snapshot.pooling_mode),
            );
            out.push_str(&format!("udb_connection_client_state{{{labels}}} 1\n"));
            out.push_str(&format!(
                "udb_connection_client_active_leases{{{labels}}} {}\n",
                snapshot.active_leases
            ));
            out.push_str(&format!(
                "udb_connection_client_total_leases{{{labels}}} {}\n",
                snapshot.total_leases
            ));
            out.push_str(&format!(
                "udb_connection_client_errors_total{{{labels}}} {}\n",
                snapshot.error_count
            ));
            out.push_str(&format!(
                "udb_connection_client_last_health_probe_unix_ms{{{labels}}} {}\n",
                snapshot.last_health_probe_unix_ms
            ));
            if let Some(budget) = snapshot.lease_budget {
                out.push_str(&format!(
                    "udb_connection_client_lease_budget{{{labels}}} {budget}\n"
                ));
            }
            if let Some(active) = snapshot.pg_active_connections {
                out.push_str(&format!(
                    "udb_connection_pg_active_connections{{{labels}}} {active}\n"
                ));
            }
            if let Some(idle) = snapshot.pg_idle_connections {
                out.push_str(&format!(
                    "udb_connection_pg_idle_connections{{{labels}}} {idle}\n"
                ));
            }
        }
        out
    }

    fn entry(&self, backend: &str, instance: &str) -> Option<ManagedClientEntry> {
        self.entry_any_state(backend, instance)
            .filter(|entry| entry.state == ClientState::Ready)
    }

    fn entry_any_state(&self, backend: &str, instance: &str) -> Option<ManagedClientEntry> {
        let backend = normalize_part(backend, "unknown");
        let instance = normalize_part(instance, "default");
        self.inner.read().ok().and_then(|guard| {
            guard
                .values()
                .find(|entry| entry.key.backend == backend && entry.key.instance == instance)
                .cloned()
        })
    }

    fn find_key(&self, backend: &str, instance: &str) -> Option<ClientKey> {
        let backend = normalize_part(backend, "unknown");
        let instance = normalize_part(instance, "default");
        self.inner.read().ok().and_then(|guard| {
            guard
                .keys()
                .find(|key| key.backend == backend && key.instance == instance)
                .cloned()
        })
    }
}

impl ManagedClientEntry {
    fn lease_budget_allows(&self) -> bool {
        self.lease_budget
            .map(|budget| self.active_leases.load(Ordering::Relaxed) < budget)
            .unwrap_or(true)
    }
}

fn normalize_part(value: &str, default: &str) -> String {
    let value = value.trim();
    if value.is_empty() {
        default.to_string()
    } else {
        value.to_ascii_lowercase()
    }
}

fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

fn metric_escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

fn lease_budget_from_labels(labels: &HashMap<String, String>) -> Option<u64> {
    let cap = labels
        .get("connection_cap")
        .or_else(|| labels.get("max_connections"))
        .or_else(|| labels.get("upstream_max_connections"))
        .and_then(|value| value.trim().parse::<u64>().ok())?;
    if cap == 0 {
        return None;
    }
    let safety_factor = labels
        .get("safety_factor")
        .or_else(|| labels.get("connection_safety_factor"))
        .and_then(|value| value.trim().parse::<f64>().ok())
        .filter(|value| *value > 0.0 && *value <= 1.0)
        .unwrap_or(1.0);
    Some(((cap as f64) * safety_factor).floor().max(1.0) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{ErrorDetail, ErrorKind};
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;
    use prost::Message as _;
    use sqlx::postgres::PgPoolOptions;

    fn decode_detail(status: &Status) -> ErrorDetail {
        let raw = status
            .metadata()
            .get_bin(ERROR_DETAIL_METADATA_KEY)
            .expect("error-detail trailer present")
            .to_bytes()
            .expect("trailer decodes to bytes");
        crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
    }

    fn manager_with_budgets<I, K, V>(pairs: I) -> ConnectionManager
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        ConnectionManager {
            inner: Arc::new(RwLock::new(HashMap::new())),
            budgets: Arc::new(TenantBudgetConfig::from_pairs(pairs)),
            tenant_slots: Arc::new(Mutex::new(HashMap::new())),
            metrics: None,
        }
    }

    /// Minimal `MetricsRecorder` that records the per-tenant starvation-gauge
    /// toggles so a test can assert the gauge fires on the admission hot path with
    /// a (bounded) tenant label. All other trait methods keep their no-op defaults.
    // `MetricsRecorder` has a `Debug` supertrait bound, so the test mock derives it.
    #[derive(Default, Debug)]
    struct RecordingRecorder {
        starved: Mutex<Vec<(String, bool)>>,
    }

    impl MetricsRecorder for RecordingRecorder {
        fn inc_runs_total(&self, _status: &str) {}

        fn inc_operations_total(&self, _kind: &str, _schema: &str, _safety: &str) {}

        fn observe_file_duration(&self, _schema: &str, _seconds: f64) {}

        fn set_blocked_operations(&self, _schema: &str, _kind: &str, _count: i64) {}

        fn inc_lint_warnings(&self, _kind: &str) {}

        fn observe_run_duration(&self, _status: &str, _seconds: f64) {}

        fn set_pending_files(&self, _count: i64) {}

        fn set_cdc_is_leader(&self, _host: &str, _is_leader: bool) {}

        fn inc_cdc_wal_messages_received_total(&self) {}

        fn inc_cdc_events_published_total(&self, _topic: &str) {}

        fn inc_cdc_errors_total(&self, _reason: &str) {}

        fn set_cdc_lag_seconds(&self, _seconds: f64) {}

        fn set_cdc_outbox_depth(&self, _depth: i64) {}

        fn set_cdc_dlq_depth(&self, _depth: i64) {}

        fn observe_cdc_publish_duration_seconds(&self, _seconds: f64) {}

        fn inc_cdc_dlq_replayed_total(&self) {}

        fn inc_cdc_duplicate_skipped_total(&self) {}

        fn set_saga_active(&self, _count: i64) {}

        fn inc_saga_compensated_total(&self) {}

        fn inc_saga_failed_compensations_total(&self) {}

        fn observe_saga_duration_seconds(&self, _seconds: f64) {}

        fn set_projection_tasks_pending(
            &self,
            _backend: &str,
            _instance: &str,
            _kind: &str,
            _count: i64,
        ) {
        }

        fn inc_projection_tasks_completed_total(
            &self,
            _backend: &str,
            _instance: &str,
            _kind: &str,
        ) {
        }

        fn inc_projection_tasks_failed_total(&self, _backend: &str, _instance: &str, _kind: &str) {}

        fn observe_projection_lag_seconds(
            &self,
            _backend: &str,
            _instance: &str,
            _kind: &str,
            _seconds: f64,
        ) {
        }

        fn inc_projection_reconciliation_repairs_total(&self, _backend: &str, _instance: &str) {}

        fn set_projection_oldest_pending_age_seconds(
            &self,
            _project: &str,
            _backend: &str,
            _instance: &str,
            _kind: &str,
            _age_seconds: f64,
        ) {
        }

        fn set_tenant_connection_starved(&self, tenant: &str, starved: bool) {
            self.starved
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push((tenant.to_string(), starved));
        }
    }

    #[test]
    fn tenant_budget_config_parses_override_default_and_timeout() {
        let cfg = TenantBudgetConfig::from_pairs([
            ("UDB_TENANT_ACME_CONN_BUDGET", "5"),
            ("UDB_TENANT_CONN_BUDGET_DEFAULT", "2"),
            ("UDB_TENANT_CONN_QUEUE_TIMEOUT_MS", "75"),
        ]);
        // Tenant-specific override wins over the default.
        assert_eq!(cfg.budget_for("acme"), Some(5));
        // Unknown tenant falls back to the default budget.
        assert_eq!(cfg.budget_for("other"), Some(2));
        assert_eq!(cfg.queue_timeout_ms, 75);
    }

    #[test]
    fn tenant_budget_is_none_when_unconfigured() {
        let cfg = TenantBudgetConfig::from_pairs(std::iter::empty::<(String, String)>());
        assert_eq!(cfg.budget_for("acme"), None);
        // The bounded queue deadline falls back to the named default constant.
        assert_eq!(cfg.queue_timeout_ms, TENANT_CONN_QUEUE_TIMEOUT_MS);
    }

    #[test]
    fn tenant_budget_lookup_is_env_part_normalized() {
        let cfg = TenantBudgetConfig::from_pairs([("UDB_TENANT_ACME_CORP_CONN_BUDGET", "3")]);
        // Non-alphanumeric chars in the tenant id normalize to `_`, matching the
        // env var name, so callers pass the raw tenant id.
        assert_eq!(cfg.budget_for("acme-corp"), Some(3));
        assert_eq!(cfg.budget_for("acme.corp"), Some(3));
    }

    #[test]
    fn tenant_budget_ignores_zero_and_unrelated_keys() {
        let cfg = TenantBudgetConfig::from_pairs([
            ("UDB_TENANT_CONN_BUDGET_DEFAULT", "0"),
            ("UDB_TENANT_ZED_CONN_BUDGET", "0"),
            ("UDB_UNRELATED", "9"),
        ]);
        // Zero budgets are treated as "no cap", not "deny everything".
        assert_eq!(cfg.budget_for("zed"), None);
        assert_eq!(cfg.budget_for("anyone"), None);
    }

    #[tokio::test]
    async fn tenant_at_budget_queues_then_rejects_on_deadline() {
        let manager = manager_with_budgets([
            ("UDB_TENANT_CONN_BUDGET_DEFAULT", "1"),
            ("UDB_TENANT_CONN_QUEUE_TIMEOUT_MS", "30"),
        ]);
        let first = manager
            .acquire_tenant_connection(Some("t1"))
            .await
            .unwrap()
            .expect("budgeted tenant gets a slot");
        // Same tenant at budget queues on the deadline and then fails closed.
        let err = manager
            .acquire_tenant_connection(Some("t1"))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::ResourceExhausted);
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Quota as i32);
        assert!(detail.retryable);
        assert_eq!(detail.retry_after_ms, 30);
        assert_eq!(detail.backend, "connection_manager");
        assert_eq!(detail.operation, "tenant_connection_budget");
        // A DIFFERENT tenant is unaffected — one tenant at budget does not starve
        // others (its slot lives on a separate semaphore).
        assert!(
            manager
                .acquire_tenant_connection(Some("t2"))
                .await
                .unwrap()
                .is_some()
        );
        // Releasing the slot lets the first tenant back in.
        drop(first);
        assert!(
            manager
                .acquire_tenant_connection(Some("t1"))
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn unconfigured_tenant_is_unlimited() {
        let manager = manager_with_budgets(std::iter::empty::<(String, String)>());
        let a = manager.acquire_tenant_connection(Some("t1")).await.unwrap();
        let b = manager.acquire_tenant_connection(Some("t1")).await.unwrap();
        // No budget → no per-tenant cap → no permits handed out.
        assert!(a.is_none() && b.is_none());
        // A missing/blank tenant id is also unbudgeted.
        assert!(
            manager
                .acquire_tenant_connection(None)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            manager
                .acquire_tenant_connection(Some("   "))
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn tenant_budget_queue_admits_when_slot_frees_before_deadline() {
        let manager = manager_with_budgets([
            ("UDB_TENANT_CONN_BUDGET_DEFAULT", "1"),
            ("UDB_TENANT_CONN_QUEUE_TIMEOUT_MS", "500"),
        ]);
        let held = manager
            .acquire_tenant_connection(Some("t1"))
            .await
            .unwrap()
            .expect("first slot");
        let waiter_manager = manager.clone();
        let waiter =
            tokio::spawn(async move { waiter_manager.acquire_tenant_connection(Some("t1")).await });
        tokio::time::sleep(Duration::from_millis(25)).await;
        // Still queued — the budget is full and the deadline has not elapsed.
        assert!(!waiter.is_finished());
        drop(held);
        // Slot returned within the deadline → the queued admission proceeds.
        let permit = waiter.await.unwrap().unwrap();
        assert!(permit.is_some());
    }

    #[tokio::test]
    async fn starvation_gauge_toggles_around_a_queued_admission() {
        let recorder = Arc::new(RecordingRecorder::default());
        let manager = manager_with_budgets([
            ("UDB_TENANT_CONN_BUDGET_DEFAULT", "1"),
            ("UDB_TENANT_CONN_QUEUE_TIMEOUT_MS", "20"),
        ])
        .with_metrics(recorder.clone());
        // First admission takes the only slot on the fast path WITHOUT touching the
        // gauge.
        let _held = manager
            .acquire_tenant_connection(Some("acme"))
            .await
            .unwrap()
            .expect("first slot");
        // Second admission for the same tenant is at budget → it queues (gauge=1)
        // and, since the slot never frees, fails closed on the deadline (gauge→0).
        let err = manager
            .acquire_tenant_connection(Some("acme"))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::ResourceExhausted);
        let events = recorder
            .starved
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        // Gauge fired starved=true while queued, then cleared to false on rejection.
        assert_eq!(
            events,
            vec![("acme".to_string(), true), ("acme".to_string(), false)]
        );
        // Every event carries only the tenant id — the single label `bounded_label`
        // caps into one bounded series (no raw tenant-id cardinality explosion).
        assert!(events.iter().all(|(tenant, _)| tenant == "acme"));
    }

    #[test]
    fn pooling_mode_reads_backend_labels() {
        assert_eq!(
            PoolingMode::from_labels(&HashMap::from([(
                "pooling_mode".to_string(),
                "transaction".to_string()
            )])),
            PoolingMode::Transaction
        );
        assert_eq!(PoolingMode::from_value("pipelined"), PoolingMode::Statement);
        assert_eq!(PoolingMode::from_value("unknown"), PoolingMode::Session);
    }

    #[tokio::test]
    async fn postgres_registration_produces_snapshot_and_metrics() {
        let pool = PgPoolOptions::new()
            .connect_lazy("postgres://user:pass@localhost/db")
            .expect("lazy pool should not connect");
        let manager = ConnectionManager::default();
        manager.register_postgres(
            "primary",
            "read_write",
            pool,
            HashMap::from([("pooling_mode".to_string(), "session".to_string())]),
        );

        let snapshots = manager.snapshots();
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].backend, "postgres");
        assert_eq!(snapshots[0].instance, "primary");
        assert_eq!(snapshots[0].pooling_mode, "session");
        assert_eq!(snapshots[0].state, "ready");
        assert!(
            manager
                .metrics_text()
                .contains("udb_connection_client_state")
        );
    }

    #[tokio::test]
    async fn postgres_lease_accounting_drops_back_to_zero() {
        let pool = PgPoolOptions::new()
            .connect_lazy("postgres://user:pass@localhost/db")
            .expect("lazy pool should not connect");
        let manager = ConnectionManager::default();
        manager.register_postgres("primary", "read_write", pool, HashMap::new());
        {
            let _lease = manager.lease_postgres("primary").unwrap();
            let snapshot = manager.snapshots().pop().unwrap();
            assert_eq!(snapshot.active_leases, 1);
            assert_eq!(snapshot.total_leases, 1);
        }
        let snapshot = manager.snapshots().pop().unwrap();
        assert_eq!(snapshot.active_leases, 0);
        assert_eq!(snapshot.total_leases, 1);
    }

    #[tokio::test]
    async fn draining_clients_stop_new_leases_but_keep_accounting() {
        let pool = PgPoolOptions::new()
            .connect_lazy("postgres://user:pass@localhost/db")
            .expect("lazy pool should not connect");
        let manager = ConnectionManager::default();
        manager.register_postgres("primary", "read_write", pool, HashMap::new());
        let lease = manager.lease_postgres("primary").unwrap();
        assert!(manager.mark_draining("postgres", "primary"));
        assert!(manager.lease_postgres("primary").is_none());
        assert_eq!(manager.active_leases("postgres", "primary"), Some(1));
        drop(lease);
        assert_eq!(manager.active_leases("postgres", "primary"), Some(0));
    }

    #[tokio::test]
    async fn health_errors_and_budget_are_reported() {
        let pool = PgPoolOptions::new()
            .connect_lazy("postgres://user:pass@localhost/db")
            .expect("lazy pool should not connect");
        let manager = ConnectionManager::default();
        manager.register_postgres(
            "primary",
            "read_write",
            pool,
            HashMap::from([
                ("connection_cap".to_string(), "2".to_string()),
                ("safety_factor".to_string(), "0.5".to_string()),
            ]),
        );
        assert_eq!(manager.snapshots()[0].lease_budget, Some(1));
        let lease = manager.lease_postgres("primary").unwrap();
        assert!(manager.lease_postgres("primary").is_none());
        drop(lease);

        assert!(manager.record_health_result(
            "postgres",
            "primary",
            false,
            Some("probe failed".to_string())
        ));
        let snapshot = manager.snapshots().pop().unwrap();
        assert_eq!(snapshot.state, "unhealthy");
        assert_eq!(snapshot.error_count, 1);
        assert_eq!(snapshot.last_error.as_deref(), Some("probe failed"));
        assert!(snapshot.last_health_probe_unix_ms > 0);
    }

    #[tokio::test]
    async fn shadow_manager_can_replace_current_registry() {
        let old_pool = PgPoolOptions::new()
            .connect_lazy("postgres://user:pass@localhost/old")
            .expect("lazy pool should not connect");
        let new_pool = PgPoolOptions::new()
            .connect_lazy("postgres://user:pass@localhost/new")
            .expect("lazy pool should not connect");
        let current = ConnectionManager::default();
        current.register_postgres("primary", "read_write", old_pool, HashMap::new());
        let shadow = ConnectionManager::default();
        shadow.register_postgres(
            "primary",
            "read_write",
            new_pool,
            HashMap::from([("generation".to_string(), "2".to_string())]),
        );

        current.replace_all_from(&shadow);
        let snapshots = current.snapshots();
        assert_eq!(snapshots.len(), 1);
        assert_eq!(
            snapshots[0].labels.get("generation").map(String::as_str),
            Some("2")
        );
    }

    #[tokio::test]
    async fn shadow_replacement_retains_draining_clients_with_active_leases() {
        let old_pool = PgPoolOptions::new()
            .connect_lazy("postgres://user:pass@localhost/old")
            .expect("lazy pool should not connect");
        let new_pool = PgPoolOptions::new()
            .connect_lazy("postgres://user:pass@localhost/new")
            .expect("lazy pool should not connect");
        let current = ConnectionManager::default();
        current.register_postgres("primary", "read_write", old_pool, HashMap::new());
        let lease = current.lease_postgres("primary").unwrap();

        let shadow = ConnectionManager::default();
        shadow.register_postgres(
            "primary",
            "read_write",
            new_pool,
            HashMap::from([("generation".to_string(), "2".to_string())]),
        );
        current.replace_all_from(&shadow);

        let snapshots = current.snapshots();
        assert_eq!(snapshots.len(), 2);
        assert!(snapshots.iter().any(|snapshot| {
            snapshot.instance == "primary"
                && snapshot.labels.get("generation").map(String::as_str) == Some("2")
        }));
        assert!(snapshots.iter().any(|snapshot| {
            snapshot.state == "draining" && snapshot.instance.starts_with("primary__draining_")
        }));
        drop(lease);
    }

    #[test]
    fn hot_path_handlers_do_not_open_backend_connections() {
        let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let mut files = vec![manifest_dir.join("src/runtime/core/tx_object.rs")];
        let handlers_dir = manifest_dir.join("src/runtime/service");
        for entry in std::fs::read_dir(&handlers_dir).expect("read service dir") {
            let path = entry.expect("read service entry").path();
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if name.starts_with("handlers_") && name.ends_with(".rs") {
                files.push(path);
            }
        }

        let forbidden = [
            "connect(",
            "Client::open",
            "from_env(",
            "std::env::var",
            "env::var",
        ];
        let mut violations = Vec::new();
        for file in files {
            let content = std::fs::read_to_string(&file).expect("read source file");
            for pattern in forbidden {
                if content.contains(pattern) {
                    violations.push(format!("{} contains {}", file.display(), pattern));
                }
            }
        }
        assert!(
            violations.is_empty(),
            "hot path connection/env access violations:\n{}",
            violations.join("\n")
        );
    }

    #[test]
    fn runtime_env_reads_are_confined_to_startup_config_or_compat_boundaries() {
        let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let runtime_dir = manifest_dir.join("src/runtime");
        let allowed = [
            // Stage 1 auth config — `AuthnConfig::from_env` reads `UDB_SESSION_*`
            // at construction, the same startup-config category as `security.rs`.
            "src/runtime/authn/",
            "src/runtime/authz/",
            "src/runtime/canonical_store/dialect.rs",
            "src/runtime/cdc/mod.rs",
            "src/runtime/cdc/source.rs",
            "src/runtime/cdc/encryption.rs",
            "src/runtime/cdc/engine_dlq.rs",
            "src/runtime/cdc/engine_tail.rs",
            "src/runtime/channels.rs",
            "src/runtime/config/",
            // Consistency fence reads `UDB_CONSISTENCY_FENCE_POLL_MS`
            // at fence-construction time as a runtime tuning knob —
            // same allowlist category as the other startup-config
            // boundaries.
            "src/runtime/consistency_fence.rs",
            // XA recovery worker reads `UDB_XA_RECOVERY_INTERVAL_SECS`
            // and `UDB_XA_RECOVERY_MAX_ATTEMPTS` at `RecoveryConfig::default`
            // construction — startup-config knob, same category.
            "src/runtime/xa_recovery.rs",
            // Compliance-evidence export worker resolves its object-store target
            // (backend/bucket/prefix/project), batch size, and cadence ONCE in
            // `EvidenceExportConfig::from_env` at spawn time — startup-config knob,
            // same category as `xa_recovery.rs` above. The worker body reads no env.
            "src/runtime/evidence_export.rs",
            "src/runtime/core/helpers.rs",
            "src/runtime/core/setup_data.rs",
            // Native-store discovery reads `UDB_NATIVE_STORE` (operator override of
            // the proto-derived native-service persistence backend) once via OnceLock
            // — startup-config knob, same category as the other UDB_* tuning reads.
            "src/runtime/core/native_store.rs",
            "src/runtime/executor_utils.rs",
            "src/runtime/executors/clickhouse.rs",
            "src/runtime/executors/http.rs",
            "src/runtime/executors/mssql.rs",
            "src/runtime/executors/mongodb.rs",
            "src/runtime/executors/neo4j.rs",
            // Native-service catalog reads `UDB_NATIVE_AUTH` (native services
            // on/off) at startup — same startup-config category as `authn/`.
            "src/runtime/native_catalog.rs",
            "src/runtime/observability.rs",
            // OTel init reads `UDB_OTEL_ENABLED` / `OTEL_EXPORTER_OTLP_ENDPOINT`
            // at startup to decide whether to install the OTLP exporter — same
            // startup-config category as `observability.rs`.
            "src/runtime/otel.rs",
            // Enterprise preflight reads UDB_* prereqs (session/password/encryption
            // secrets, header-scopes) ONCE at startup + in `udb doctor --enterprise`
            // to report the whole unmet set — same startup-config category.
            "src/runtime/preflight.rs",
            "src/runtime/projection/mod.rs",
            "src/runtime/replica.rs",
            "src/runtime/security.rs",
            "src/runtime/signalling/",
            "src/runtime/singleton.rs",
            "src/runtime/service/mod.rs",
            "src/runtime/service/asset_service/",
            // Auth service handlers construct config via `AuthnConfig::from_env`
            // (in their inline tests) — same startup-config category as
            // `service/mod.rs` and `security.rs`. Folder-module prefix so it
            // covers `auth_service/mod.rs` and submodules.
            "src/runtime/service/auth_service/",
            "src/runtime/service/method_security.rs",
            // Notification service reads `UDB_NOTIFICATION_TEST_MODE` via a OnceLock
            // gate (test-mode FAILED-log affordance) — same native-service
            // startup-config category as asset/storage/webrtc/auth above.
            "src/runtime/service/notification_service/",
            "src/runtime/service/storage_service/",
            "src/runtime/service/webrtc_service/",
            // ConfigService resolves `UDB_CONFIG_EVAL_TTL_SECONDS` ONCE via a
            // OnceLock (server-authoritative eval TTL) — startup-config category.
            "src/runtime/service/config_service/",
            // LiveQueryService resolves `LIVEQUERY_BUFFER_EVENTS` ONCE via a
            // OnceLock (process-static bounded delta-buffer depth) — same category.
            "src/runtime/service/livequery_service/",
            // BackupService reads `UDB_STORAGE_OBJECT_BACKEND`/`UDB_STORAGE_BUCKET`
            // once in `build_backup_service` at startup (the default object-store
            // target) — same startup-config category as `storage_service` above.
            "src/runtime/service/backup_service/",
            // Webhook/Vault/Metering/Embedding/Cache services resolve native
            // service knobs through OnceLock-backed helpers or live-test fixtures;
            // these stay in the native service startup/config category.
            "src/runtime/service/webhook_service/",
            "src/runtime/service/vault_service/",
            "src/runtime/service/metering_service/",
            "src/runtime/service/embedding_service/",
            "src/runtime/service/cache_service/",
        ];
        let mut stack = vec![runtime_dir];
        let mut violations = Vec::new();
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir).expect("read runtime dir") {
                let path = entry.expect("read runtime entry").path();
                if path.is_dir() {
                    stack.push(path);
                    continue;
                }
                if path.extension().and_then(|ext| ext.to_str()) != Some("rs") {
                    continue;
                }
                let rel = path
                    .strip_prefix(&manifest_dir)
                    .expect("runtime source under manifest")
                    .to_string_lossy()
                    .replace('\\', "/");
                if rel == "src/runtime/connection_manager.rs"
                    || rel.contains("/tests.rs")
                    || rel.contains("_tests.rs")
                    || rel.contains("tests/")
                    || allowed.iter().any(|prefix| rel.starts_with(prefix))
                {
                    continue;
                }
                let content = std::fs::read_to_string(&path).expect("read runtime source");
                for pattern in ["std::env::var", "env::var", "from_env("] {
                    if content.contains(pattern) {
                        violations.push(format!("{rel} contains {pattern}"));
                    }
                }
            }
        }
        assert!(
            violations.is_empty(),
            "runtime env access outside allowlisted startup/config boundaries:\n{}",
            violations.join("\n")
        );
    }
}
