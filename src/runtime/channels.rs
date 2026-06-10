use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tonic::Status;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OperationChannel {
    Read,
    Write,
    Transaction,
    Migration,
    Cdc,
    Admin,
    Vector,
    Object,
    GenericDispatch,
}

impl OperationChannel {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
            Self::Transaction => "tx",
            Self::Migration => "migration",
            Self::Cdc => "cdc",
            Self::Admin => "admin",
            Self::Vector => "vector",
            Self::Object => "object",
            Self::GenericDispatch => "generic",
        }
    }

    fn env_suffix(&self) -> &'static str {
        match self {
            Self::Read => "READ",
            Self::Write => "WRITE",
            Self::Transaction => "TX",
            Self::Migration => "MIGRATION",
            Self::Cdc => "CDC",
            Self::Admin => "ADMIN",
            Self::Vector => "VECTOR",
            Self::Object => "OBJECT",
            Self::GenericDispatch => "GENERIC",
        }
    }

    fn default_queue_timeout_ms(&self) -> u64 {
        match self {
            Self::Read | Self::Write | Self::Vector | Self::Object | Self::GenericDispatch => 250,
            Self::Transaction | Self::Migration | Self::Cdc | Self::Admin => 0,
        }
    }

    pub fn default_cost(&self) -> u32 {
        match self {
            Self::Read => 1,
            Self::Write => 2,
            Self::Transaction => 6,
            Self::Migration => 20,
            Self::Cdc => 2,
            Self::Admin => 1,
            Self::Vector => 4,
            Self::Object => 4,
            Self::GenericDispatch => 3,
        }
    }
}

#[derive(Debug)]
pub struct ChannelPermit {
    _base: OwnedSemaphorePermit,
    _tenant: Option<OwnedSemaphorePermit>,
    _project: Option<OwnedSemaphorePermit>,
    _instance: Option<OwnedSemaphorePermit>,
    _backend_instance: Option<OwnedSemaphorePermit>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScopeControl {
    Active,
    Paused,
    Draining,
    Shed,
}

impl ScopeControl {
    pub fn from_value(value: &str) -> Option<Self> {
        match value.to_ascii_lowercase().replace('-', "_").as_str() {
            "active" | "resume" | "resumed" | "clear" => Some(Self::Active),
            "paused" | "pause" => Some(Self::Paused),
            "draining" | "drain" => Some(Self::Draining),
            "shed" | "shedding" | "reject" => Some(Self::Shed),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FairAdmissionSnapshot {
    pub key: String,
    pub tokens_available: u64,
    pub burst: u64,
}

/// Per-operation + per-tenant/project/instance concurrency control with bounded
/// queueing (backpressure → `ResourceExhausted`).
///
/// **Scope (urgent_fix #39):** these semaphores and fairness maps are
/// **instance-local**. Behind a load balancer, each broker replica enforces its
/// OWN limits, so the effective fleet-wide concurrency for a tenant is
/// `configured_limit × replica_count`, not `configured_limit`. This is intentional
/// for the current single-process admission model; true fleet-wide limits require a
/// shared (e.g. Redis-backed) token store. Size per-replica limits with the replica
/// count in mind, or front the broker with an LB-level per-tenant rate limit.
#[derive(Debug, Clone)]
pub struct ChannelManager {
    read_sem: Arc<Semaphore>,
    write_sem: Arc<Semaphore>,
    tx_sem: Arc<Semaphore>,
    migration_sem: Arc<Semaphore>,
    cdc_sem: Arc<Semaphore>,
    admin_sem: Arc<Semaphore>,
    vector_sem: Arc<Semaphore>,
    object_sem: Arc<Semaphore>,
    generic_sem: Arc<Semaphore>,
    tenant_sems: ScopeSemMap,
    project_sems: ScopeSemMap,
    instance_sems: ScopeSemMap,
    backend_instance_sems: ScopeSemMap,
    fairness: Arc<Mutex<HashMap<String, TokenBucket>>>,
    controls: Arc<Mutex<HashMap<String, ScopeControl>>>,
    env_config: Arc<ChannelEnvConfig>,

    read_limit: usize,
    write_limit: usize,
    tx_limit: usize,
    migration_limit: usize,
    cdc_limit: usize,
    admin_limit: usize,
    vector_limit: usize,
    object_limit: usize,
    generic_limit: usize,
    pub read_timeout: u64,
    pub write_timeout: u64,
    pub tx_timeout: u64,
    pub migration_timeout: u64,
    pub cdc_timeout: u64,
    pub admin_timeout: u64,
    pub vector_timeout: u64,
    pub object_timeout: u64,
    pub generic_timeout: u64,
    read_queue_timeout_ms: u64,
    write_queue_timeout_ms: u64,
    tx_queue_timeout_ms: u64,
    migration_queue_timeout_ms: u64,
    cdc_queue_timeout_ms: u64,
    admin_queue_timeout_ms: u64,
    vector_queue_timeout_ms: u64,
    object_queue_timeout_ms: u64,
    generic_queue_timeout_ms: u64,
}

#[derive(Debug, Clone)]
struct TokenBucket {
    tokens: f64,
    last_refill: Instant,
}

#[derive(Debug)]
struct ScopedSemaphoreEntry {
    sem: Arc<Semaphore>,
    last_access: Instant,
    limit: usize,
}

impl ScopedSemaphoreEntry {
    fn has_outstanding_permits(&self) -> bool {
        self.sem.available_permits() < self.limit
    }
}

/// Per-scope semaphore map value: the semaphore plus metadata used by
/// idle-TTL eviction. (#206/#31)
type ScopeSemMap = Arc<Mutex<HashMap<String, ScopedSemaphoreEntry>>>;

/// Max live entries in any per-scope map before idle/over-cap eviction kicks in.
const SCOPE_MAP_MAX_ENTRIES: usize = 10_000;
/// Entries idle longer than this are evicted on the next eviction sweep.
const SCOPE_MAP_IDLE_TTL: std::time::Duration = std::time::Duration::from_secs(3600);

/// Bound a per-scope map (#206/#211): evict idle (> `ttl`) entries, then the
/// oldest entries until the map is back at `cap`. Lazy — only scans once the map
/// has reached `cap`, so the common small-map path stays O(1). `last` reads each value's
/// last-access instant (the sidecar `Instant` for semaphores, `last_refill`
/// for token buckets, the staged-at instant for catalogs).
fn evict_scope_map<V>(
    map: &mut HashMap<String, V>,
    cap: usize,
    ttl: std::time::Duration,
    last: impl Fn(&V) -> Instant,
    can_evict: impl Fn(&V) -> bool,
) {
    if map.len() < cap {
        return;
    }
    let now = Instant::now();
    map.retain(|_, v| !can_evict(v) || now.saturating_duration_since(last(v)) < ttl);
    if map.len() > cap {
        let mut ages: Vec<(String, Instant)> = map
            .iter()
            .filter(|(_, v)| can_evict(v))
            .map(|(k, v)| (k.clone(), last(v)))
            .collect();
        ages.sort_by_key(|(_, t)| *t);
        let to_remove = map.len() - cap;
        for (k, _) in ages.into_iter().take(to_remove) {
            map.remove(&k);
        }
    }
}

#[derive(Debug, Clone, Default)]
struct ChannelEnvConfig {
    usize_values: HashMap<String, usize>,
    u64_values: HashMap<String, u64>,
    f64_values: HashMap<String, f64>,
    scope_controls: HashMap<String, ScopeControl>,
}

impl ChannelEnvConfig {
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
        let usize_values = raw
            .iter()
            .filter_map(|(key, value)| value.parse().ok().map(|parsed| (key.clone(), parsed)))
            .collect();
        let u64_values = raw
            .iter()
            .filter_map(|(key, value)| value.parse().ok().map(|parsed| (key.clone(), parsed)))
            .collect();
        let f64_values = raw
            .iter()
            .filter_map(|(key, value)| value.parse().ok().map(|parsed| (key.clone(), parsed)))
            .collect();
        let mut scope_controls = HashMap::new();
        for (suffix, control) in [
            ("SHED", ScopeControl::Shed),
            ("DRAINING", ScopeControl::Draining),
            ("PAUSED", ScopeControl::Paused),
        ] {
            for (scope, key) in [
                ("tenant", format!("UDB_{suffix}_TENANTS")),
                ("project", format!("UDB_{suffix}_PROJECTS")),
            ] {
                if let Some(value) = raw.get(&key) {
                    for item in value
                        .split(',')
                        .map(str::trim)
                        .filter(|item| !item.is_empty())
                    {
                        scope_controls.insert(format!("{scope}:{}", env_part(item)), control);
                    }
                }
            }
        }
        Self {
            usize_values,
            u64_values,
            f64_values,
            scope_controls,
        }
    }

    fn usize_for(&self, keys: &[String]) -> Option<usize> {
        keys.iter()
            .find_map(|key| self.usize_values.get(key).copied())
    }

    fn u64_for(&self, keys: &[String]) -> Option<u64> {
        keys.iter()
            .find_map(|key| self.u64_values.get(key).copied())
    }

    fn f64_for(&self, keys: &[String]) -> Option<f64> {
        keys.iter()
            .find_map(|key| self.f64_values.get(key).copied())
    }

    fn scope_control(&self, scope: &str, value: &str) -> Option<ScopeControl> {
        self.scope_controls
            .get(&format!("{scope}:{}", env_part(value)))
            .copied()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ChannelSettings {
    pub read_max_concurrent: usize,
    pub write_max_concurrent: usize,
    pub tx_max_concurrent: usize,
    pub migration_max_concurrent: usize,
    pub cdc_max_concurrent: usize,
    pub admin_max_concurrent: usize,
    pub vector_max_concurrent: usize,
    pub object_max_concurrent: usize,
    pub generic_max_concurrent: usize,
    pub read_timeout_secs: u64,
    pub write_timeout_secs: u64,
    pub tx_timeout_secs: u64,
    pub migration_timeout_secs: u64,
    pub cdc_timeout_secs: u64,
    pub admin_timeout_secs: u64,
    pub vector_timeout_secs: u64,
    pub object_timeout_secs: u64,
    pub generic_timeout_secs: u64,
    pub read_queue_timeout_ms: u64,
    pub write_queue_timeout_ms: u64,
    pub tx_queue_timeout_ms: u64,
    pub migration_queue_timeout_ms: u64,
    pub cdc_queue_timeout_ms: u64,
    pub admin_queue_timeout_ms: u64,
    pub vector_queue_timeout_ms: u64,
    pub object_queue_timeout_ms: u64,
    pub generic_queue_timeout_ms: u64,
}

impl Default for ChannelSettings {
    fn default() -> Self {
        Self {
            read_max_concurrent: 300,
            write_max_concurrent: 100,
            tx_max_concurrent: 50,
            migration_max_concurrent: 1,
            cdc_max_concurrent: 32,
            admin_max_concurrent: 20,
            vector_max_concurrent: 80,
            object_max_concurrent: 80,
            generic_max_concurrent: 50,
            read_timeout_secs: 10,
            write_timeout_secs: 20,
            tx_timeout_secs: 30,
            migration_timeout_secs: 300,
            cdc_timeout_secs: 30,
            admin_timeout_secs: 15,
            vector_timeout_secs: 20,
            object_timeout_secs: 60,
            generic_timeout_secs: 30,
            read_queue_timeout_ms: OperationChannel::Read.default_queue_timeout_ms(),
            write_queue_timeout_ms: OperationChannel::Write.default_queue_timeout_ms(),
            tx_queue_timeout_ms: OperationChannel::Transaction.default_queue_timeout_ms(),
            migration_queue_timeout_ms: OperationChannel::Migration.default_queue_timeout_ms(),
            cdc_queue_timeout_ms: OperationChannel::Cdc.default_queue_timeout_ms(),
            admin_queue_timeout_ms: OperationChannel::Admin.default_queue_timeout_ms(),
            vector_queue_timeout_ms: OperationChannel::Vector.default_queue_timeout_ms(),
            object_queue_timeout_ms: OperationChannel::Object.default_queue_timeout_ms(),
            generic_queue_timeout_ms: OperationChannel::GenericDispatch.default_queue_timeout_ms(),
        }
    }
}

impl ChannelSettings {
    pub fn merge_env(&mut self) {
        let get_limit = |env_var: &str| -> Option<usize> {
            env::var(env_var).ok().and_then(|v| v.parse().ok())
        };

        let get_timeout =
            |env_var: &str| -> Option<u64> { env::var(env_var).ok().and_then(|v| v.parse().ok()) };

        if let Some(v) = get_limit("UDB_READ_MAX_CONCURRENT") {
            self.read_max_concurrent = v;
        }
        if let Some(v) = get_limit("UDB_WRITE_MAX_CONCURRENT") {
            self.write_max_concurrent = v;
        }
        if let Some(v) = get_limit("UDB_TX_MAX_CONCURRENT") {
            self.tx_max_concurrent = v;
        }
        if let Some(v) = get_limit("UDB_MIGRATION_MAX_CONCURRENT") {
            self.migration_max_concurrent = v;
        }
        if let Some(v) = get_limit("UDB_CDC_MAX_CONCURRENT") {
            self.cdc_max_concurrent = v;
        }
        if let Some(v) = get_limit("UDB_ADMIN_MAX_CONCURRENT") {
            self.admin_max_concurrent = v;
        }
        if let Some(v) = get_limit("UDB_VECTOR_MAX_CONCURRENT") {
            self.vector_max_concurrent = v;
        }
        if let Some(v) = get_limit("UDB_OBJECT_MAX_CONCURRENT") {
            self.object_max_concurrent = v;
        }
        if let Some(v) = get_limit("UDB_GENERIC_MAX_CONCURRENT") {
            self.generic_max_concurrent = v;
        }
        if let Some(v) = get_timeout("UDB_READ_TIMEOUT_SECS") {
            self.read_timeout_secs = v;
        }
        if let Some(v) = get_timeout("UDB_WRITE_TIMEOUT_SECS") {
            self.write_timeout_secs = v;
        }
        if let Some(v) = get_timeout("UDB_TX_TIMEOUT_SECS") {
            self.tx_timeout_secs = v;
        }
        if let Some(v) = get_timeout("UDB_MIGRATION_TIMEOUT_SECS") {
            self.migration_timeout_secs = v;
        }
        if let Some(v) = get_timeout("UDB_CDC_TIMEOUT_SECS") {
            self.cdc_timeout_secs = v;
        }
        if let Some(v) = get_timeout("UDB_ADMIN_TIMEOUT_SECS") {
            self.admin_timeout_secs = v;
        }
        if let Some(v) = get_timeout("UDB_VECTOR_TIMEOUT_SECS") {
            self.vector_timeout_secs = v;
        }
        if let Some(v) = get_timeout("UDB_OBJECT_TIMEOUT_SECS") {
            self.object_timeout_secs = v;
        }
        if let Some(v) = get_timeout("UDB_GENERIC_TIMEOUT_SECS") {
            self.generic_timeout_secs = v;
        }
        if let Some(v) = get_timeout("UDB_READ_QUEUE_TIMEOUT_MS") {
            self.read_queue_timeout_ms = v;
        }
        if let Some(v) = get_timeout("UDB_WRITE_QUEUE_TIMEOUT_MS") {
            self.write_queue_timeout_ms = v;
        }
        if let Some(v) = get_timeout("UDB_TX_QUEUE_TIMEOUT_MS") {
            self.tx_queue_timeout_ms = v;
        }
        if let Some(v) = get_timeout("UDB_MIGRATION_QUEUE_TIMEOUT_MS") {
            self.migration_queue_timeout_ms = v;
        }
        if let Some(v) = get_timeout("UDB_CDC_QUEUE_TIMEOUT_MS") {
            self.cdc_queue_timeout_ms = v;
        }
        if let Some(v) = get_timeout("UDB_ADMIN_QUEUE_TIMEOUT_MS") {
            self.admin_queue_timeout_ms = v;
        }
        if let Some(v) = get_timeout("UDB_VECTOR_QUEUE_TIMEOUT_MS") {
            self.vector_queue_timeout_ms = v;
        }
        if let Some(v) = get_timeout("UDB_OBJECT_QUEUE_TIMEOUT_MS") {
            self.object_queue_timeout_ms = v;
        }
        if let Some(v) = get_timeout("UDB_GENERIC_QUEUE_TIMEOUT_MS") {
            self.generic_queue_timeout_ms = v;
        }
    }
}

impl Default for ChannelManager {
    fn default() -> Self {
        Self::from_env()
    }
}

impl ChannelManager {
    pub fn from_env() -> Self {
        let mut settings = ChannelSettings::default();
        settings.merge_env();
        Self::from_settings(&settings)
    }

    pub fn from_settings(settings: &ChannelSettings) -> Self {
        Self::from_settings_with_env_config(settings, ChannelEnvConfig::from_env())
    }

    fn from_settings_with_env_config(
        settings: &ChannelSettings,
        env_config: ChannelEnvConfig,
    ) -> Self {
        let read_limit = configured_base_limit(
            &env_config,
            OperationChannel::Read,
            settings.read_max_concurrent,
        );
        let write_limit = configured_base_limit(
            &env_config,
            OperationChannel::Write,
            settings.write_max_concurrent,
        );
        let tx_limit = configured_base_limit(
            &env_config,
            OperationChannel::Transaction,
            settings.tx_max_concurrent,
        );
        let migration_limit = configured_base_limit(
            &env_config,
            OperationChannel::Migration,
            settings.migration_max_concurrent,
        );
        let cdc_limit = configured_base_limit(
            &env_config,
            OperationChannel::Cdc,
            settings.cdc_max_concurrent,
        );
        let admin_limit = configured_base_limit(
            &env_config,
            OperationChannel::Admin,
            settings.admin_max_concurrent,
        );
        let vector_limit = configured_base_limit(
            &env_config,
            OperationChannel::Vector,
            settings.vector_max_concurrent,
        );
        let object_limit = configured_base_limit(
            &env_config,
            OperationChannel::Object,
            settings.object_max_concurrent,
        );
        let generic_limit = configured_base_limit(
            &env_config,
            OperationChannel::GenericDispatch,
            settings.generic_max_concurrent,
        );
        Self {
            read_sem: Arc::new(Semaphore::new(settings.read_max_concurrent)),
            write_sem: Arc::new(Semaphore::new(settings.write_max_concurrent)),
            tx_sem: Arc::new(Semaphore::new(settings.tx_max_concurrent)),
            migration_sem: Arc::new(Semaphore::new(settings.migration_max_concurrent)),
            cdc_sem: Arc::new(Semaphore::new(settings.cdc_max_concurrent)),
            admin_sem: Arc::new(Semaphore::new(settings.admin_max_concurrent)),
            vector_sem: Arc::new(Semaphore::new(settings.vector_max_concurrent)),
            object_sem: Arc::new(Semaphore::new(settings.object_max_concurrent)),
            generic_sem: Arc::new(Semaphore::new(settings.generic_max_concurrent)),
            tenant_sems: Arc::new(Mutex::new(HashMap::new())),
            project_sems: Arc::new(Mutex::new(HashMap::new())),
            instance_sems: Arc::new(Mutex::new(HashMap::new())),
            backend_instance_sems: Arc::new(Mutex::new(HashMap::new())),
            fairness: Arc::new(Mutex::new(HashMap::new())),
            controls: Arc::new(Mutex::new(HashMap::new())),
            env_config: Arc::new(env_config),

            read_limit,
            write_limit,
            tx_limit,
            migration_limit,
            cdc_limit,
            admin_limit,
            vector_limit,
            object_limit,
            generic_limit,
            read_timeout: settings.read_timeout_secs,
            write_timeout: settings.write_timeout_secs,
            tx_timeout: settings.tx_timeout_secs,
            migration_timeout: settings.migration_timeout_secs,
            cdc_timeout: settings.cdc_timeout_secs,
            admin_timeout: settings.admin_timeout_secs,
            vector_timeout: settings.vector_timeout_secs,
            object_timeout: settings.object_timeout_secs,
            generic_timeout: settings.generic_timeout_secs,
            read_queue_timeout_ms: settings.read_queue_timeout_ms,
            write_queue_timeout_ms: settings.write_queue_timeout_ms,
            tx_queue_timeout_ms: settings.tx_queue_timeout_ms,
            migration_queue_timeout_ms: settings.migration_queue_timeout_ms,
            cdc_queue_timeout_ms: settings.cdc_queue_timeout_ms,
            admin_queue_timeout_ms: settings.admin_queue_timeout_ms,
            vector_queue_timeout_ms: settings.vector_queue_timeout_ms,
            object_queue_timeout_ms: settings.object_queue_timeout_ms,
            generic_queue_timeout_ms: settings.generic_queue_timeout_ms,
        }
    }

    #[allow(clippy::result_large_err)]
    pub fn acquire(&self, op: OperationChannel) -> Result<ChannelPermit, Status> {
        let sem = self.base_sem(op);

        sem.clone()
            .try_acquire_owned()
            .map(|permit| ChannelPermit {
                _base: permit,
                _tenant: None,
                _project: None,
                _instance: None,
                _backend_instance: None,
            })
            .map_err(|_| Status::resource_exhausted(format!("{} channel overloaded", op.as_str())))
    }

    #[allow(clippy::result_large_err)]
    pub async fn acquire_with_backpressure(
        &self,
        op: OperationChannel,
    ) -> Result<ChannelPermit, Status> {
        self.acquire_scoped_with_backpressure(op, None, None).await
    }

    #[allow(clippy::result_large_err)]
    pub async fn acquire_scoped_with_backpressure(
        &self,
        op: OperationChannel,
        tenant_id: Option<&str>,
        instance_name: Option<&str>,
    ) -> Result<ChannelPermit, Status> {
        self.acquire_fair_with_backpressure(
            op,
            tenant_id,
            None,
            None,
            instance_name,
            op.default_cost(),
        )
        .await
    }

    #[allow(clippy::result_large_err)]
    pub async fn acquire_fair_with_backpressure(
        &self,
        op: OperationChannel,
        tenant_id: Option<&str>,
        project_id: Option<&str>,
        backend: Option<&str>,
        instance_name: Option<&str>,
        cost: u32,
    ) -> Result<ChannelPermit, Status> {
        self.check_controls(tenant_id, project_id)?;
        self.acquire_budget(op, tenant_id, project_id, backend, instance_name, cost)?;
        let timeout = self.queue_timeout_ms(op);
        let base = self
            .acquire_one(op, self.base_sem(op), timeout, "channel")
            .await?;
        let tenant = match scoped_value(tenant_id) {
            Some(tenant) => {
                let sem = self.scoped_sem(
                    &self.tenant_sems,
                    format!("tenant:{}:{}", tenant, op.as_str()),
                    self.tenant_limit(op, tenant),
                );
                Some(self.acquire_one(op, sem, timeout, "tenant channel").await?)
            }
            None => None,
        };
        let project = match scoped_value(project_id) {
            Some(project) => {
                let sem = self.scoped_sem(
                    &self.project_sems,
                    format!("project:{}:{}", project, op.as_str()),
                    self.project_limit(op, project),
                );
                Some(
                    self.acquire_one(op, sem, timeout, "project channel")
                        .await?,
                )
            }
            None => None,
        };
        let instance = match scoped_value(instance_name) {
            Some(instance) => {
                let sem = self.scoped_sem(
                    &self.instance_sems,
                    format!("instance:{}:{}", instance, op.as_str()),
                    self.instance_limit(op, instance),
                );
                Some(
                    self.acquire_one(op, sem, timeout, "instance channel")
                        .await?,
                )
            }
            None => None,
        };
        let backend_instance = match (scoped_value(backend), scoped_value(instance_name)) {
            (Some(backend), Some(instance)) => {
                let key = format!("{}:{}", backend, instance);
                let sem = self.scoped_sem(
                    &self.backend_instance_sems,
                    format!("backend-instance:{}:{}", key, op.as_str()),
                    self.backend_instance_limit(op, backend, instance),
                );
                Some(
                    self.acquire_one(op, sem, timeout, "backend instance channel")
                        .await?,
                )
            }
            _ => None,
        };
        Ok(ChannelPermit {
            _base: base,
            _tenant: tenant,
            _project: project,
            _instance: instance,
            _backend_instance: backend_instance,
        })
    }

    fn base_sem(&self, op: OperationChannel) -> Arc<Semaphore> {
        match op {
            OperationChannel::Read => &self.read_sem,
            OperationChannel::Write => &self.write_sem,
            OperationChannel::Transaction => &self.tx_sem,
            OperationChannel::Migration => &self.migration_sem,
            OperationChannel::Cdc => &self.cdc_sem,
            OperationChannel::Admin => &self.admin_sem,
            OperationChannel::Vector => &self.vector_sem,
            OperationChannel::Object => &self.object_sem,
            OperationChannel::GenericDispatch => &self.generic_sem,
        }
        .clone()
    }

    async fn acquire_one(
        &self,
        op: OperationChannel,
        sem: Arc<Semaphore>,
        queue_timeout_ms: u64,
        label: &str,
    ) -> Result<OwnedSemaphorePermit, Status> {
        if queue_timeout_ms == 0 {
            return sem.clone().try_acquire_owned().map_err(|_| {
                Status::resource_exhausted(format!("{} {label} overloaded", op.as_str()))
            });
        }
        match tokio::time::timeout(Duration::from_millis(queue_timeout_ms), sem.acquire_owned())
            .await
        {
            Ok(Ok(permit)) => Ok(permit),
            Ok(Err(_)) => Err(Status::resource_exhausted(format!(
                "{} {label} closed",
                op.as_str()
            ))),
            Err(_) => Err(Status::resource_exhausted(format!(
                "{} {label} overloaded after waiting {}ms",
                op.as_str(),
                queue_timeout_ms
            ))),
        }
    }

    fn scoped_sem(&self, map: &ScopeSemMap, key: String, limit: usize) -> Arc<Semaphore> {
        let mut guard = map.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        // #206: bound the map before inserting a new scope's semaphore.
        if !guard.contains_key(&key) {
            evict_scope_map(
                &mut guard,
                SCOPE_MAP_MAX_ENTRIES.saturating_sub(1),
                SCOPE_MAP_IDLE_TTL,
                |entry| entry.last_access,
                |entry| !entry.has_outstanding_permits(),
            );
        }
        let now = Instant::now();
        let entry = guard.entry(key).or_insert_with(|| ScopedSemaphoreEntry {
            sem: Arc::new(Semaphore::new(limit)),
            last_access: now,
            limit,
        });
        entry.last_access = now;
        Arc::clone(&entry.sem)
    }

    fn tenant_limit(&self, op: OperationChannel, tenant: &str) -> usize {
        self.env_config
            .usize_for(&[
                format!(
                    "UDB_CHANNEL_TENANT_{}_{}_LIMIT",
                    env_part(tenant),
                    op.env_suffix()
                ),
                format!(
                    "UDB_TENANT_{}_{}_MAX_CONCURRENT",
                    env_part(tenant),
                    op.env_suffix()
                ),
            ])
            .unwrap_or_else(|| self.base_limit(op))
    }

    fn project_limit(&self, op: OperationChannel, project: &str) -> usize {
        self.env_config
            .usize_for(&[
                format!(
                    "UDB_CHANNEL_PROJECT_{}_{}_LIMIT",
                    env_part(project),
                    op.env_suffix()
                ),
                format!(
                    "UDB_PROJECT_{}_{}_MAX_CONCURRENT",
                    env_part(project),
                    op.env_suffix()
                ),
            ])
            .unwrap_or_else(|| self.base_limit(op))
    }

    fn instance_limit(&self, op: OperationChannel, instance: &str) -> usize {
        self.env_config
            .usize_for(&[
                format!(
                    "UDB_CHANNEL_INSTANCE_{}_{}_LIMIT",
                    env_part(instance),
                    op.env_suffix()
                ),
                format!(
                    "UDB_INSTANCE_{}_{}_MAX_CONCURRENT",
                    env_part(instance),
                    op.env_suffix()
                ),
            ])
            .unwrap_or_else(|| self.base_limit(op))
    }

    fn backend_instance_limit(&self, op: OperationChannel, backend: &str, instance: &str) -> usize {
        self.env_config
            .usize_for(&[
                format!(
                    "UDB_CHANNEL_BACKEND_INSTANCE_{}_{}_{}_LIMIT",
                    env_part(backend),
                    env_part(instance),
                    op.env_suffix()
                ),
                format!(
                    "UDB_BACKEND_INSTANCE_{}_{}_{}_MAX_CONCURRENT",
                    env_part(backend),
                    env_part(instance),
                    op.env_suffix()
                ),
            ])
            .unwrap_or_else(|| self.base_limit(op))
    }

    fn check_controls(
        &self,
        tenant_id: Option<&str>,
        project_id: Option<&str>,
    ) -> Result<(), Status> {
        for (scope, value) in [
            ("tenant", scoped_value(tenant_id)),
            ("project", scoped_value(project_id)),
        ] {
            if let Some(value) = value {
                let control = self.scope_control(scope, value);
                match control {
                    ScopeControl::Active => {}
                    ScopeControl::Paused => {
                        return Err(Status::unavailable(format!(
                            "{scope} '{value}' is paused; retry after 30s"
                        )));
                    }
                    ScopeControl::Draining => {
                        return Err(Status::resource_exhausted(format!(
                            "{scope} '{value}' is draining and not accepting new work; retry after 30s"
                        )));
                    }
                    ScopeControl::Shed => {
                        return Err(Status::resource_exhausted(format!(
                            "{scope} '{value}' is shedding load; retry after 5s"
                        )));
                    }
                }
            }
        }
        Ok(())
    }

    fn scope_control(&self, scope: &str, value: &str) -> ScopeControl {
        let key = format!("{scope}:{}", env_part(value));
        if let Some(control) = self
            .controls
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&key)
            .copied()
        {
            return control;
        }
        self.env_config
            .scope_control(scope, value)
            .unwrap_or(ScopeControl::Active)
    }

    pub fn set_scope_control(&self, scope: &str, value: &str, control: ScopeControl) {
        let key = format!("{}:{}", scope.to_ascii_lowercase(), env_part(value));
        let mut controls = self
            .controls
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if control == ScopeControl::Active {
            controls.remove(&key);
        } else {
            controls.insert(key, control);
        }
    }

    pub fn clear_scope_control(&self, scope: &str, value: &str) {
        self.set_scope_control(scope, value, ScopeControl::Active);
    }

    pub fn fair_admission_snapshots(&self) -> Vec<FairAdmissionSnapshot> {
        let guard = self
            .fairness
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard
            .iter()
            .map(|(key, bucket)| {
                let (rate, burst) = self.bucket_settings(OperationChannel::Read);
                FairAdmissionSnapshot {
                    key: key.clone(),
                    tokens_available: bucket.tokens.max(0.0).floor() as u64,
                    burst: burst.max(rate).ceil() as u64,
                }
            })
            .collect()
    }

    fn acquire_budget(
        &self,
        op: OperationChannel,
        tenant_id: Option<&str>,
        project_id: Option<&str>,
        backend: Option<&str>,
        instance_name: Option<&str>,
        cost: u32,
    ) -> Result<(), Status> {
        let Some(tenant) = scoped_value(tenant_id).or(Some("anonymous")) else {
            return Ok(());
        };
        let project = scoped_value(project_id).unwrap_or("default");
        let backend = scoped_value(backend).unwrap_or("default");
        let instance = scoped_value(instance_name).unwrap_or("default");
        let key = format!(
            "project={};tenant={};op={};backend={};instance={}",
            env_part(project),
            env_part(tenant),
            op.as_str(),
            env_part(backend),
            env_part(instance)
        );
        let (rate, burst) = self.bucket_settings(op);
        let weighted_cost =
            (cost.max(1) as f64) / self.fairness_weight(project, tenant, op).max(0.1);
        let mut guard = self
            .fairness
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // #206: bound the fairness map (token buckets carry their own
        // `last_refill` instant, which doubles as the last-access time).
        if !guard.contains_key(&key) {
            evict_scope_map(
                &mut guard,
                SCOPE_MAP_MAX_ENTRIES.saturating_sub(1),
                SCOPE_MAP_IDLE_TTL,
                |b: &TokenBucket| b.last_refill,
                |_| true,
            );
        }
        let bucket = guard.entry(key).or_insert_with(|| TokenBucket {
            tokens: burst,
            last_refill: Instant::now(),
        });
        let now = Instant::now();
        let elapsed = now
            .checked_duration_since(bucket.last_refill)
            .unwrap_or_default()
            .as_secs_f64();
        bucket.tokens = (bucket.tokens + elapsed * rate).min(burst);
        bucket.last_refill = now;
        if bucket.tokens < weighted_cost {
            let wait_secs = ((weighted_cost - bucket.tokens) / rate.max(0.001)).ceil() as u64;
            return Err(Status::resource_exhausted(format!(
                "{} fair queue budget exhausted for project '{project}' tenant '{tenant}'; retry after {}s",
                op.as_str(),
                wait_secs.max(1)
            )));
        }
        bucket.tokens -= weighted_cost;
        Ok(())
    }

    fn base_limit(&self, op: OperationChannel) -> usize {
        match op {
            OperationChannel::Read => self.read_limit,
            OperationChannel::Write => self.write_limit,
            OperationChannel::Transaction => self.tx_limit,
            OperationChannel::Migration => self.migration_limit,
            OperationChannel::Cdc => self.cdc_limit,
            OperationChannel::Admin => self.admin_limit,
            OperationChannel::Vector => self.vector_limit,
            OperationChannel::Object => self.object_limit,
            OperationChannel::GenericDispatch => self.generic_limit,
        }
    }

    pub fn queue_timeout_ms(&self, op: OperationChannel) -> u64 {
        match op {
            OperationChannel::Read => self.read_queue_timeout_ms,
            OperationChannel::Write => self.write_queue_timeout_ms,
            OperationChannel::Transaction => self.tx_queue_timeout_ms,
            OperationChannel::Migration => self.migration_queue_timeout_ms,
            OperationChannel::Cdc => self.cdc_queue_timeout_ms,
            OperationChannel::Admin => self.admin_queue_timeout_ms,
            OperationChannel::Vector => self.vector_queue_timeout_ms,
            OperationChannel::Object => self.object_queue_timeout_ms,
            OperationChannel::GenericDispatch => self.generic_queue_timeout_ms,
        }
    }

    pub fn deadline_secs(&self, op: OperationChannel, backend: Option<&str>) -> u64 {
        if let Some(backend) = scoped_value(backend)
            && let Some(value) = self.env_config.u64_for(&[
                format!(
                    "UDB_DEADLINE_{}_{}_SECS",
                    env_part(backend),
                    op.env_suffix()
                ),
                format!("UDB_{}_{}_TIMEOUT_SECS", env_part(backend), op.env_suffix()),
            ])
        {
            return value;
        }
        self.timeout_secs(op)
    }

    fn bucket_settings(&self, op: OperationChannel) -> (f64, f64) {
        let rate = self
            .env_config
            .f64_for(&[
                format!("UDB_FAIR_{}_TOKENS_PER_SEC", op.env_suffix()),
                "UDB_FAIR_TOKENS_PER_SEC".to_string(),
            ])
            .unwrap_or(100.0)
            .max(0.001);
        let burst = self
            .env_config
            .f64_for(&[
                format!("UDB_FAIR_{}_BURST", op.env_suffix()),
                "UDB_FAIR_BURST".to_string(),
            ])
            .unwrap_or(rate * 2.0)
            .max(rate);
        (rate, burst)
    }

    fn fairness_weight(&self, project: &str, tenant: &str, op: OperationChannel) -> f64 {
        self.env_config
            .f64_for(&[
                format!(
                    "UDB_FAIR_PROJECT_{}_TENANT_{}_{}_WEIGHT",
                    env_part(project),
                    env_part(tenant),
                    op.env_suffix()
                ),
                format!(
                    "UDB_FAIR_TENANT_{}_{}_WEIGHT",
                    env_part(tenant),
                    op.env_suffix()
                ),
                format!(
                    "UDB_FAIR_PROJECT_{}_{}_WEIGHT",
                    env_part(project),
                    op.env_suffix()
                ),
                "UDB_FAIR_DEFAULT_WEIGHT".to_string(),
            ])
            .unwrap_or(1.0)
    }

    pub fn timeout_secs(&self, op: OperationChannel) -> u64 {
        match op {
            OperationChannel::Read => self.read_timeout,
            OperationChannel::Write => self.write_timeout,
            OperationChannel::Transaction => self.tx_timeout,
            OperationChannel::Migration => self.migration_timeout,
            OperationChannel::Cdc => self.cdc_timeout,
            OperationChannel::Admin => self.admin_timeout,
            OperationChannel::Vector => self.vector_timeout,
            OperationChannel::Object => self.object_timeout,
            OperationChannel::GenericDispatch => self.generic_timeout,
        }
    }
}

fn scoped_value(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

fn env_part(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect()
}

fn configured_base_limit(
    env_config: &ChannelEnvConfig,
    op: OperationChannel,
    settings_limit: usize,
) -> usize {
    env_config
        .usize_for(&[
            format!("UDB_CHANNEL_{}_LIMIT", op.env_suffix()),
            format!("UDB_{}_MAX_CONCURRENT", op.env_suffix()),
        ])
        .unwrap_or(settings_limit)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::Code;

    fn test_manager(limit: usize) -> ChannelManager {
        test_manager_with_env(limit, [])
    }

    fn test_manager_with_env<const N: usize>(
        limit: usize,
        pairs: [(&str, &str); N],
    ) -> ChannelManager {
        let sem = || Arc::new(Semaphore::new(limit));
        ChannelManager {
            read_sem: sem(),
            write_sem: sem(),
            tx_sem: sem(),
            migration_sem: sem(),
            cdc_sem: sem(),
            admin_sem: sem(),
            vector_sem: sem(),
            object_sem: sem(),
            generic_sem: sem(),
            tenant_sems: Arc::new(Mutex::new(HashMap::new())),
            project_sems: Arc::new(Mutex::new(HashMap::new())),
            instance_sems: Arc::new(Mutex::new(HashMap::new())),
            backend_instance_sems: Arc::new(Mutex::new(HashMap::new())),
            fairness: Arc::new(Mutex::new(HashMap::new())),
            controls: Arc::new(Mutex::new(HashMap::new())),
            env_config: Arc::new(ChannelEnvConfig::from_pairs(pairs)),
            read_limit: limit,
            write_limit: limit,
            tx_limit: limit,
            migration_limit: limit,
            cdc_limit: limit,
            admin_limit: limit,
            vector_limit: limit,
            object_limit: limit,
            generic_limit: limit,
            read_timeout: 1,
            write_timeout: 2,
            tx_timeout: 3,
            migration_timeout: 4,
            cdc_timeout: 5,
            admin_timeout: 6,
            vector_timeout: 7,
            object_timeout: 8,
            generic_timeout: 9,
            read_queue_timeout_ms: 100,
            write_queue_timeout_ms: 100,
            tx_queue_timeout_ms: 100,
            migration_queue_timeout_ms: 100,
            cdc_queue_timeout_ms: 100,
            admin_queue_timeout_ms: 100,
            vector_queue_timeout_ms: 100,
            object_queue_timeout_ms: 100,
            generic_queue_timeout_ms: 100,
        }
    }

    #[test]
    fn acquire_returns_resource_exhausted_when_channel_is_full() {
        let manager = test_manager(1);
        let _permit = manager.acquire(OperationChannel::Read).unwrap();
        let err = manager.acquire(OperationChannel::Read).unwrap_err();
        assert_eq!(err.code(), Code::ResourceExhausted);
        assert!(err.message().contains("read channel overloaded"));
    }

    #[test]
    fn operation_channel_labels_and_timeouts_are_stable() {
        let manager = test_manager(2);
        assert_eq!(OperationChannel::Transaction.as_str(), "tx");
        assert_eq!(OperationChannel::GenericDispatch.as_str(), "generic");
        assert_eq!(manager.timeout_secs(OperationChannel::Read), 1);
        assert_eq!(manager.timeout_secs(OperationChannel::Write), 2);
        assert_eq!(manager.timeout_secs(OperationChannel::Transaction), 3);
        assert_eq!(manager.timeout_secs(OperationChannel::Migration), 4);
        assert_eq!(manager.timeout_secs(OperationChannel::Cdc), 5);
        assert_eq!(manager.timeout_secs(OperationChannel::Admin), 6);
        assert_eq!(manager.timeout_secs(OperationChannel::Vector), 7);
        assert_eq!(manager.timeout_secs(OperationChannel::Object), 8);
        assert_eq!(manager.timeout_secs(OperationChannel::GenericDispatch), 9);
    }

    // ── Phase 12: Per-channel semaphore rejection tests ───────────────────────

    #[test]
    fn all_channels_reject_when_full() {
        let channels = [
            OperationChannel::Read,
            OperationChannel::Write,
            OperationChannel::Transaction,
            OperationChannel::Migration,
            OperationChannel::Cdc,
            OperationChannel::Admin,
            OperationChannel::Vector,
            OperationChannel::Object,
            OperationChannel::GenericDispatch,
        ];
        let manager = test_manager(1);
        for channel in channels {
            let label = channel.as_str();
            let _permit = manager
                .acquire(channel)
                .unwrap_or_else(|_| panic!("first acquire on {label} should succeed"));
            let err = match manager.acquire(channel) {
                Ok(_) => panic!("second acquire on {label} should fail"),
                Err(err) => err,
            };
            assert_eq!(
                err.code(),
                Code::ResourceExhausted,
                "{label} should return ResourceExhausted"
            );
            assert!(
                err.message().contains(label),
                "{label} error message should contain channel name, got: {}",
                err.message()
            );
        }
    }

    #[test]
    fn permit_release_allows_reacquire() {
        let manager = test_manager(1);
        {
            let _permit = manager.acquire(OperationChannel::Write).unwrap();
            // permit drops here
        }
        // After drop, should be acquirable again
        assert!(manager.acquire(OperationChannel::Write).is_ok());
    }

    #[test]
    fn channel_with_limit_zero_always_rejects() {
        let manager = test_manager(0);
        let err = manager.acquire(OperationChannel::Read).unwrap_err();
        assert_eq!(err.code(), Code::ResourceExhausted);
    }

    #[test]
    fn configured_base_limit_uses_cached_channel_alias() {
        let env_config = ChannelEnvConfig::from_pairs([("UDB_CHANNEL_READ_LIMIT", "7")]);
        assert_eq!(
            configured_base_limit(&env_config, OperationChannel::Read, 300),
            7
        );
    }

    #[test]
    fn scoped_semaphore_eviction_preserves_entries_with_outstanding_permits() {
        let old = Instant::now()
            .checked_sub(Duration::from_secs(7200))
            .unwrap();
        let live_sem = Arc::new(Semaphore::new(1));
        let _permit = live_sem.clone().try_acquire_owned().unwrap();
        let mut ttl_map = HashMap::from([
            (
                "live".to_string(),
                ScopedSemaphoreEntry {
                    sem: live_sem.clone(),
                    last_access: old,
                    limit: 1,
                },
            ),
            (
                "idle".to_string(),
                ScopedSemaphoreEntry {
                    sem: Arc::new(Semaphore::new(1)),
                    last_access: old,
                    limit: 1,
                },
            ),
        ]);

        evict_scope_map(
            &mut ttl_map,
            2,
            Duration::from_secs(1),
            |entry| entry.last_access,
            |entry| !entry.has_outstanding_permits(),
        );
        assert!(ttl_map.contains_key("live"));
        assert!(!ttl_map.contains_key("idle"));

        let mut cap_map = HashMap::from([
            (
                "live".to_string(),
                ScopedSemaphoreEntry {
                    sem: live_sem,
                    last_access: old,
                    limit: 1,
                },
            ),
            (
                "idle-a".to_string(),
                ScopedSemaphoreEntry {
                    sem: Arc::new(Semaphore::new(1)),
                    last_access: old + Duration::from_millis(1),
                    limit: 1,
                },
            ),
            (
                "idle-b".to_string(),
                ScopedSemaphoreEntry {
                    sem: Arc::new(Semaphore::new(1)),
                    last_access: old + Duration::from_millis(2),
                    limit: 1,
                },
            ),
        ]);
        evict_scope_map(
            &mut cap_map,
            2,
            Duration::from_secs(10_000),
            |entry| entry.last_access,
            |entry| !entry.has_outstanding_permits(),
        );
        assert!(cap_map.contains_key("live"));
        assert_eq!(cap_map.len(), 2);
    }

    #[tokio::test]
    async fn queued_acquire_waits_for_selected_channels() {
        let manager = test_manager(1);
        let permit = manager
            .acquire_with_backpressure(OperationChannel::Read)
            .await
            .unwrap();
        let clone = manager.clone();
        let waiter = tokio::spawn(async move {
            clone
                .acquire_with_backpressure(OperationChannel::Read)
                .await
        });
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert!(!waiter.is_finished());
        drop(permit);
        assert!(waiter.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn scoped_acquire_enforces_tenant_and_instance_limits() {
        let manager = test_manager_with_env(
            4,
            [
                ("UDB_CHANNEL_TENANT_T1_WRITE_LIMIT", "1"),
                ("UDB_CHANNEL_INSTANCE_A_WRITE_LIMIT", "1"),
            ],
        );
        let _permit = manager
            .acquire_scoped_with_backpressure(OperationChannel::Write, Some("t1"), Some("a"))
            .await
            .unwrap();
        let err = manager
            .acquire_scoped_with_backpressure(OperationChannel::Write, Some("t1"), Some("a"))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::ResourceExhausted);
    }

    #[tokio::test]
    async fn scoped_acquire_enforces_project_limits() {
        let manager = test_manager_with_env(4, [("UDB_CHANNEL_PROJECT_P1_READ_LIMIT", "1")]);
        let _permit = manager
            .acquire_fair_with_backpressure(
                OperationChannel::Read,
                Some("t1"),
                Some("p1"),
                Some("postgres"),
                Some("primary"),
                1,
            )
            .await
            .unwrap();
        let err = manager
            .acquire_fair_with_backpressure(
                OperationChannel::Read,
                Some("t2"),
                Some("p1"),
                Some("postgres"),
                Some("primary"),
                1,
            )
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::ResourceExhausted);
        assert!(err.message().contains("project channel"));
    }

    #[tokio::test]
    async fn fair_budget_is_isolated_by_tenant() {
        let manager = test_manager_with_env(
            8,
            [
                ("UDB_FAIR_ADMIN_TOKENS_PER_SEC", "0.001"),
                ("UDB_FAIR_ADMIN_BURST", "1"),
            ],
        );
        let first = manager
            .acquire_fair_with_backpressure(
                OperationChannel::Admin,
                Some("t1"),
                Some("p1"),
                Some("postgres"),
                Some("primary"),
                1,
            )
            .await;
        assert!(first.is_ok());
        let same_tenant = manager
            .acquire_fair_with_backpressure(
                OperationChannel::Admin,
                Some("t1"),
                Some("p1"),
                Some("postgres"),
                Some("primary"),
                1,
            )
            .await
            .unwrap_err();
        assert_eq!(same_tenant.code(), Code::ResourceExhausted);
        let other_tenant = manager
            .acquire_fair_with_backpressure(
                OperationChannel::Admin,
                Some("t2"),
                Some("p1"),
                Some("postgres"),
                Some("primary"),
                1,
            )
            .await;
        assert!(other_tenant.is_ok());
    }

    #[tokio::test]
    async fn scope_controls_pause_and_shed_work() {
        let manager = test_manager(4);
        manager.set_scope_control("tenant", "t1", ScopeControl::Paused);
        let paused = manager
            .acquire_fair_with_backpressure(
                OperationChannel::Read,
                Some("t1"),
                Some("p1"),
                Some("postgres"),
                Some("primary"),
                1,
            )
            .await
            .unwrap_err();
        assert_eq!(paused.code(), Code::Unavailable);
        manager.set_scope_control("tenant", "t1", ScopeControl::Shed);
        let shed = manager
            .acquire_fair_with_backpressure(
                OperationChannel::Read,
                Some("t1"),
                Some("p1"),
                Some("postgres"),
                Some("primary"),
                1,
            )
            .await
            .unwrap_err();
        assert_eq!(shed.code(), Code::ResourceExhausted);
        manager.clear_scope_control("tenant", "t1");
        assert!(
            manager
                .acquire_fair_with_backpressure(
                    OperationChannel::Read,
                    Some("t1"),
                    Some("p1"),
                    Some("postgres"),
                    Some("primary"),
                    1,
                )
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn env_scope_controls_are_cached_in_manager() {
        let manager = test_manager_with_env(4, [("UDB_PAUSED_TENANTS", "t1")]);
        let paused = manager
            .acquire_fair_with_backpressure(
                OperationChannel::Read,
                Some("t1"),
                Some("p1"),
                Some("postgres"),
                Some("primary"),
                1,
            )
            .await
            .unwrap_err();
        assert_eq!(paused.code(), Code::Unavailable);
    }

    #[tokio::test]
    async fn concurrent_read_write_load_uses_independent_backend_channels() {
        let manager = test_manager(2);
        let mut tasks = Vec::new();
        for index in 0..16 {
            let manager = manager.clone();
            let op = if index % 2 == 0 {
                OperationChannel::Read
            } else {
                OperationChannel::Write
            };
            tasks.push(tokio::spawn(async move {
                let _permit = manager.acquire_with_backpressure(op).await?;
                tokio::time::sleep(Duration::from_millis(5)).await;
                Ok::<_, Status>(())
            }));
        }
        for task in tasks {
            task.await.unwrap().unwrap();
        }
    }

    #[test]
    fn deadline_can_be_overridden_by_backend_and_operation() {
        let manager = test_manager_with_env(2, [("UDB_DEADLINE_QDRANT_VECTOR_SECS", "11")]);
        assert_eq!(
            manager.deadline_secs(OperationChannel::Vector, Some("qdrant")),
            11
        );
        assert_eq!(manager.deadline_secs(OperationChannel::Vector, None), 7);
    }
}
