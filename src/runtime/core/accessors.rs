//! Continuation `impl DataBrokerRuntime` block (Phase F split of core.rs).
use super::*;

/// Generate the `<backend>_for_instance` / `<backend>_for_instance_for_project`
/// resolver pair for a labelled-instance backend (MongoDB/Neo4j/ClickHouse/
/// Qdrant/S3). Every resolver shares the exact same control flow:
///
/// 1. When a non-blank instance label is supplied: enforce the per-project
///    instance allow-list, reject if any of the backend's circuit breakers are
///    open, then look the instance up in the instances map (with the `"default"`
///    fallback to the unlabelled single client).
/// 2. Otherwise: enforce the unlabelled-default policy, then pick via
///    `choose_instance_name` → instances map → single client → first available.
///
/// Per-backend variation is expressed as macro arguments: the struct fields, the
/// allow-list / circuit-breaker / choose names (S3 spans `minio`+`s3`), the
/// display labels, and the final not-configured `Status` constructor + message
/// (Qdrant uses `unavailable`, the rest `failed_precondition`). Behavior is
/// byte-for-byte identical to the previous hand-written methods.
macro_rules! impl_instance_resolver {
    (
        feature = $feature:literal,
        simple = $simple:ident,
        project = $project:ident,
        ret = $ret:ty,
        single = $single:ident,
        instances = $instances:ident,
        allow = [$($allow:literal),+ $(,)?],
        breakers = [$($breaker:literal),+ $(,)?],
        unlabeled = $unlabeled:literal,
        choose = [$($choose:literal),+ $(,)?],
        cb_label = $cb_label:literal,
        not_connected_label = $nc_label:literal,
        not_configured = $not_configured:ident($not_configured_msg:literal) $(,)?
    ) => {
        #[cfg(feature = $feature)]
        pub(crate) fn $simple(
            &self,
            instance: Option<&str>,
        ) -> Result<$ret, tonic::Status> {
            self.$project(instance, "")
        }

        #[cfg(feature = $feature)]
        pub(crate) fn $project(
            &self,
            instance: Option<&str>,
            project_id: &str,
        ) -> Result<$ret, tonic::Status> {
            if let Some(instance) = instance.filter(|value| !value.trim().is_empty()) {
                self.ensure_backend_instance_name_allowed_for_project(
                    &[$($allow),+],
                    instance,
                    project_id,
                )?;
                if $(!self.circuit_breaker_allows($breaker, Some(instance)))||+ {
                    return Err(tonic::Status::unavailable(format!(
                        "{} instance '{}' circuit breaker is open",
                        $cb_label,
                        instance
                    )));
                }
                return self
                    .$instances
                    .get(instance)
                    .or_else(|| {
                        if instance == "default" {
                            self.$single.as_ref()
                        } else {
                            None
                        }
                    })
                    .ok_or_else(|| {
                        tonic::Status::failed_precondition(format!(
                            "{} instance '{}' is not connected",
                            $nc_label,
                            instance
                        ))
                    });
            }
            self.ensure_unlabeled_default_allowed_for_project($unlabeled, project_id)?;
            None
                $(.or_else(|| self.choose_instance_name($choose, false)))+
                .and_then(|name| self.$instances.get(name))
                .or(self.$single.as_ref())
                .or_else(|| self.$instances.values().next())
                .ok_or_else(|| tonic::Status::$not_configured($not_configured_msg))
        }
    };
}

impl DataBrokerRuntime {
    pub fn planning_only() -> Self {
        Self::default()
    }

    pub fn init_report(&self) -> &RuntimeInitReport {
        &self.report
    }

    pub fn backend_instances(&self) -> &[RuntimeBackendInstance] {
        &self.backend_instances
    }

    pub fn executor_registry(&self) -> &BackendExecutorRegistry {
        &self.executor_registry
    }

    pub fn connection_manager(&self) -> &ConnectionManager {
        &self.connections
    }

    pub fn connection_snapshots(&self) -> Vec<crate::runtime::connection_manager::ClientSnapshot> {
        self.connections.snapshots()
    }

    pub async fn current_write_receipt(
        &self,
        manifest_checksum: &str,
    ) -> crate::runtime::consistency::WriteReceipt {
        // U6 + NW1-3e: route the receipt through
        // `consistency_fence::build_write_receipt` which now takes a
        // `SystemStores` trait object. The store handles
        // `current_durability_token` + `outbox_max_seq` in its own
        // dialect (PG: pg_current_wal_lsn + MAX(event_seq); MySQL:
        // GTID / binlog + LAST_INSERT_ID; SQLite: PRAGMA + max rowid).
        match self.default_system_stores_clone() {
            Some(store) => {
                crate::runtime::consistency_fence::build_write_receipt(
                    store.as_ref(),
                    manifest_checksum,
                    Vec::new(),
                )
                .await
            }
            None => crate::runtime::consistency::WriteReceipt {
                source_lsn: String::new(),
                outbox_seq: 0,
                projection_task_ids: Vec::new(),
                manifest_checksum: manifest_checksum.to_string(),
                written_at_unix_ms: unix_millis(),
            },
        }
    }

    /// Resolve a `(backend, instance)` selector to the live dispatch target
    /// after registry, connectivity, and circuit-breaker checks. Returns the
    /// canonicalised backend + instance the caller should use; combine with
    /// [`Self::resolve_dispatch_executor`] to obtain the executor itself.
    ///
    /// U2 step 6: this method replaces the old `DefaultBackendExecutor`
    /// forwarding adapter. Field names (`backend`, `instance`) on the returned
    /// struct match the adapter's so existing callers/tests are unchanged.
    pub fn backend_executor(
        &self,
        backend: &str,
        instance: Option<&str>,
    ) -> Result<ResolvedExecutorTarget, tonic::Status> {
        self.backend_executor_for_project(backend, instance, "")
    }

    pub fn backend_executor_for_project(
        &self,
        backend: &str,
        instance: Option<&str>,
        project_id: &str,
    ) -> Result<ResolvedExecutorTarget, tonic::Status> {
        let resolved = if let Some(instance) = instance {
            self.resolve_backend_selector_for_project(&format!("{backend}:{instance}"), project_id)?
        } else {
            self.resolve_backend_selector_for_project(backend, project_id)?
        };
        let registration = self
            .executor_registry
            .get(&resolved.backend, resolved.instance.as_deref())
            .filter(|registration| {
                self.circuit_breaker_allows(
                    &registration.backend,
                    registration
                        .instance
                        .as_deref()
                        .or(resolved.instance.as_deref()),
                )
            })
            .or_else(|| {
                if resolved.instance.is_some() {
                    return None;
                }
                self.executor_registry.all().find(|registration| {
                    registration.backend == resolved.backend
                        && registration.connected
                        && self.circuit_breaker_allows(
                            &registration.backend,
                            registration.instance.as_deref(),
                        )
                })
            })
            .ok_or_else(|| {
                tonic::Status::failed_precondition(format!(
                    "backend executor '{}:{}' is not registered",
                    resolved.backend,
                    resolved.instance.as_deref().unwrap_or("default")
                ))
            })?;
        if !registration.connected {
            return Err(tonic::Status::failed_precondition(format!(
                "backend executor '{}:{}' is registered but not connected",
                registration.backend,
                registration.instance.as_deref().unwrap_or("default")
            )));
        }
        let target_instance = registration.instance.clone().or(resolved.instance);
        Ok(ResolvedExecutorTarget {
            backend: registration.backend.clone(),
            instance: target_instance,
        })
    }

    pub fn enabled_backend_names(&self) -> Vec<String> {
        let mut names = Vec::new();
        for instance in &self.backend_instances {
            if instance.enabled
                && instance.connected
                && self.circuit_breaker_allows(&instance.backend, Some(&instance.name))
                && !names.contains(&instance.backend)
            {
                names.push(instance.backend.clone());
            }
        }
        if self.report.postgres_configured && !names.iter().any(|name| name == "postgres") {
            names.push("postgres".to_string());
        }
        if self.report.redis_configured && !names.iter().any(|name| name == "redis") {
            names.push("redis".to_string());
        }
        if self.report.qdrant_configured && !names.iter().any(|name| name == "qdrant") {
            names.push("qdrant".to_string());
        }
        if self.report.s3_configured && !names.iter().any(|name| name == "s3") {
            names.push("s3".to_string());
        }
        if self.report.mongodb_configured && !names.iter().any(|name| name == "mongodb") {
            names.push("mongodb".to_string());
        }
        if self.report.neo4j_configured && !names.iter().any(|name| name == "neo4j") {
            names.push("neo4j".to_string());
        }
        if self.report.clickhouse_configured && !names.iter().any(|name| name == "clickhouse") {
            names.push("clickhouse".to_string());
        }
        names
    }

    pub(crate) fn pg_pool_for_instance(
        &self,
        instance: Option<&str>,
    ) -> Result<&PgPool, tonic::Status> {
        if let Some(instance) = instance.filter(|value| !value.trim().is_empty()) {
            return self
                .pg_instances
                .get(instance)
                .or_else(|| {
                    if instance == "primary" {
                        self.pg_pool.as_ref()
                    } else {
                        None
                    }
                })
                .ok_or_else(|| {
                    tonic::Status::failed_precondition(format!(
                        "postgres instance '{instance}' is not connected"
                    ))
                });
        }
        self.pg_pool
            .as_ref()
            .or_else(|| self.pg_instances.get("primary"))
            .or_else(|| self.pg_instances.values().next())
            .ok_or_else(|| tonic::Status::failed_precondition("PostgreSQL is not configured"))
    }

    /// NW3-1: MySQL pool resolver. Mirrors `pg_pool_for_instance`.
    /// Returns the named pool, with fallback to `mysql_pool` for the
    /// `"primary"` slot.
    #[cfg(feature = "mysql")]
    pub(crate) fn mysql_pool_for_instance(&self, instance: &str) -> Option<&sqlx::MySqlPool> {
        self.mysql_instances.get(instance).or_else(|| {
            if instance == "primary" {
                self.mysql_pool.as_ref()
            } else {
                None
            }
        })
    }

    /// C9: Elasticsearch client resolver. Returns the named client,
    /// with fallback to `elasticsearch` (the primary set by
    /// `register_elasticsearch`) for the `"primary"` slot.
    #[cfg(feature = "elasticsearch")]
    pub(crate) fn elasticsearch_for_instance(
        &self,
        instance: &str,
    ) -> Option<&crate::runtime::executors::elasticsearch::ElasticsearchHttpClient> {
        self.elasticsearch_instances.get(instance).or_else(|| {
            if instance == "primary" {
                self.elasticsearch.as_ref()
            } else {
                None
            }
        })
    }

    /// C9: Memcached client resolver. Same shape as the Elasticsearch
    /// / MySQL resolvers — named instance with fallback to the
    /// `primary` slot set by `register_memcached`.
    #[cfg(feature = "memcached")]
    pub(crate) fn memcached_for_instance(
        &self,
        instance: &str,
    ) -> Option<&crate::runtime::executors::memcached::MemcachedClient> {
        self.memcached_instances.get(instance).or_else(|| {
            if instance == "primary" {
                self.memcached.as_ref()
            } else {
                None
            }
        })
    }

    /// C9: SQL Server client resolver. Same pattern.
    #[cfg(feature = "mssql")]
    pub(crate) fn mssql_for_instance(
        &self,
        instance: &str,
    ) -> Option<&crate::runtime::executors::mssql::MssqlClient> {
        self.mssql_instances.get(instance).or_else(|| {
            if instance == "primary" {
                self.mssql.as_ref()
            } else {
                None
            }
        })
    }

    /// C9: Weaviate client resolver.
    #[cfg(feature = "weaviate")]
    pub(crate) fn weaviate_for_instance(
        &self,
        instance: &str,
    ) -> Option<&crate::runtime::executors::weaviate::WeaviateHttpClient> {
        self.weaviate_instances.get(instance).or_else(|| {
            if instance == "primary" {
                self.weaviate.as_ref()
            } else {
                None
            }
        })
    }

    /// C9: Pinecone client resolver.
    #[cfg(feature = "pinecone")]
    pub(crate) fn pinecone_for_instance(
        &self,
        instance: &str,
    ) -> Option<&crate::runtime::executors::pinecone::PineconeHttpClient> {
        self.pinecone_instances.get(instance).or_else(|| {
            if instance == "primary" {
                self.pinecone.as_ref()
            } else {
                None
            }
        })
    }

    #[cfg(feature = "cassandra")]
    pub(crate) fn cassandra_for_instance(
        &self,
        instance: &str,
    ) -> Option<&crate::runtime::executors::cassandra::CassandraClient> {
        self.cassandra_instances.get(instance).or_else(|| {
            if instance == "primary" {
                self.cassandra.as_ref()
            } else {
                None
            }
        })
    }

    #[cfg(feature = "azureblob")]
    pub(crate) fn azureblob_for_instance(
        &self,
        instance: &str,
    ) -> Option<&crate::runtime::executors::azureblob::AzureBlobClient> {
        self.azureblob_instances.get(instance).or_else(|| {
            if instance == "primary" {
                self.azureblob.as_ref()
            } else {
                None
            }
        })
    }

    #[cfg(feature = "gcs")]
    pub(crate) fn gcs_for_instance(
        &self,
        instance: &str,
    ) -> Option<&crate::runtime::executors::gcs::GcsClient> {
        self.gcs_instances.get(instance).or_else(|| {
            if instance == "primary" {
                self.gcs.as_ref()
            } else {
                None
            }
        })
    }

    /// NW3-2: SQLite pool resolver.
    #[cfg(feature = "sqlite")]
    pub(crate) fn sqlite_pool_for_instance(&self, instance: &str) -> Option<&sqlx::SqlitePool> {
        self.sqlite_instances.get(instance).or_else(|| {
            if instance == "primary" {
                self.sqlite_pool.as_ref()
            } else {
                None
            }
        })
    }

    pub fn resolve_backend_selector(
        &self,
        selector: &str,
    ) -> Result<ResolvedBackendSelector, tonic::Status> {
        self.resolve_backend_selector_for_project(selector, "")
    }

    pub fn resolve_backend_selector_for_project(
        &self,
        selector: &str,
        project_id: &str,
    ) -> Result<ResolvedBackendSelector, tonic::Status> {
        let (backend_raw, instance_raw) = split_backend_selector(selector);
        let backend = crate::planning::backend::BackendKind::from_store_kind("", backend_raw)
            .map(|kind| kind.as_str().to_string())
            .ok_or_else(|| {
                tonic::Status::invalid_argument(format!("unknown backend '{selector}'"))
            })?;
        let instance = instance_raw.map(str::to_string);
        if let Some(instance_name) = &instance {
            let Some(runtime_instance) = self
                .backend_instances
                .iter()
                .find(|candidate| candidate.backend == backend && candidate.name == *instance_name)
            else {
                if backend == "postgres" && self.pg_pool_for_instance(Some(instance_name)).is_ok() {
                    self.allow_unlabeled_fallback_instance_for_project(
                        &backend,
                        instance_name,
                        project_id,
                    )?;
                    return Ok(ResolvedBackendSelector { backend, instance });
                }
                return Err(tonic::Status::not_found(format!(
                    "backend instance '{}:{}' is not configured",
                    backend, instance_name
                )));
            };
            self.ensure_instance_matches_project(runtime_instance, project_id)?;
            if !runtime_instance.enabled {
                return Err(tonic::Status::failed_precondition(format!(
                    "backend instance '{}:{}' is disabled",
                    backend, instance_name
                )));
            }
            if !runtime_instance.connected {
                return Err(tonic::Status::failed_precondition(format!(
                    "backend instance '{}:{}' is configured but not connected",
                    backend, instance_name
                )));
            }
            if !self.circuit_breaker_allows(&backend, Some(instance_name)) {
                return Err(tonic::Status::unavailable(format!(
                    "backend instance '{}:{}' circuit breaker is open",
                    backend, instance_name
                )));
            }
        }
        Ok(ResolvedBackendSelector { backend, instance })
    }

    pub fn resolve_backend_targets(
        &self,
        selector: &str,
        spec_json: &str,
    ) -> Result<Vec<ResolvedBackendSelector>, tonic::Status> {
        self.resolve_backend_targets_for_project(selector, spec_json, "")
    }

    pub fn resolve_backend_targets_for_project(
        &self,
        selector: &str,
        spec_json: &str,
        project_id: &str,
    ) -> Result<Vec<ResolvedBackendSelector>, tonic::Status> {
        let (backend_raw, instance_raw) = split_backend_selector(selector);
        let backend = crate::planning::backend::BackendKind::from_store_kind("", backend_raw)
            .map(|kind| kind.as_str().to_string())
            .ok_or_else(|| {
                tonic::Status::invalid_argument(format!("unknown backend '{selector}'"))
            })?;
        let labels_filter = parse_dispatch_json(spec_json)
            .ok()
            .and_then(|spec| spec.get("target_labels").cloned())
            .and_then(|value| value.as_object().cloned());
        let wildcard = matches!(instance_raw, Some("*" | "all")) || labels_filter.is_some();
        if !wildcard {
            return self
                .resolve_backend_selector_for_project(selector, project_id)
                .map(|target| vec![target]);
        }

        let mut targets = Vec::new();
        for instance in self.backend_instances.iter().filter(|candidate| {
            candidate.backend == backend
                && candidate.enabled
                && candidate.connected
                && self.circuit_breaker_allows(&candidate.backend, Some(&candidate.name))
                && self.instance_matches_project(candidate, project_id)
        }) {
            let labels_match = labels_filter.as_ref().is_none_or(|labels| {
                labels.iter().all(|(key, value)| {
                    value.as_str().is_some_and(|expected| {
                        instance
                            .labels
                            .get(key)
                            .map(|actual| actual == expected)
                            .unwrap_or(false)
                    })
                })
            });
            if labels_match {
                targets.push(ResolvedBackendSelector {
                    backend: backend.clone(),
                    instance: Some(instance.name.clone()),
                });
            }
        }
        if targets.is_empty() {
            return Err(tonic::Status::not_found(format!(
                "no connected backend instances matched '{selector}'"
            )));
        }
        Ok(targets)
    }

    pub fn cache_metrics_snapshot(&self) -> CacheMetricSnapshot {
        self.cache_metrics.snapshot()
    }

    pub fn channels(&self) -> &crate::runtime::channels::ChannelManager {
        &self.channels
    }

    pub(crate) fn saga_compensator_registry(
        &self,
    ) -> std::sync::Arc<crate::runtime::saga_compensators::CompensatorRegistry> {
        let mut registry = crate::runtime::saga_compensators::CompensatorRegistry::new();
        #[cfg(feature = "qdrant")]
        {
            if let Some(client) = self.qdrant.clone() {
                registry.register(std::sync::Arc::new(
                    crate::runtime::saga_compensators::QdrantPointCompensator::new(client),
                ));
            }
            for client in self.qdrant_instances.values().cloned() {
                registry.register(std::sync::Arc::new(
                    crate::runtime::saga_compensators::QdrantPointCompensator::new(client),
                ));
            }
        }
        #[cfg(feature = "s3")]
        {
            if let Some(client) = self.s3.clone() {
                registry.register(std::sync::Arc::new(
                    crate::runtime::saga_compensators::S3ObjectCompensator::new(
                        "s3",
                        client.clone(),
                    ),
                ));
                registry.register(std::sync::Arc::new(
                    crate::runtime::saga_compensators::S3ObjectCompensator::new("minio", client),
                ));
            }
            for client in self.s3_instances.values().cloned() {
                registry.register(std::sync::Arc::new(
                    crate::runtime::saga_compensators::S3ObjectCompensator::new(
                        "s3",
                        client.clone(),
                    ),
                ));
                registry.register(std::sync::Arc::new(
                    crate::runtime::saga_compensators::S3ObjectCompensator::new("minio", client),
                ));
            }
        }
        #[cfg(feature = "mongodb")]
        {
            if let Some(executor) = self.mongodb.clone() {
                registry.register(std::sync::Arc::new(
                    crate::runtime::saga_compensators::MongoDbCompensator::new(executor),
                ));
            }
            for executor in self.mongodb_instances.values().cloned() {
                registry.register(std::sync::Arc::new(
                    crate::runtime::saga_compensators::MongoDbCompensator::new(executor),
                ));
            }
        }
        #[cfg(feature = "neo4j")]
        {
            if let Some(executor) = self.neo4j.clone() {
                registry.register(std::sync::Arc::new(
                    crate::runtime::saga_compensators::Neo4jCompensator::new(executor),
                ));
            }
            for executor in self.neo4j_instances.values().cloned() {
                registry.register(std::sync::Arc::new(
                    crate::runtime::saga_compensators::Neo4jCompensator::new(executor),
                ));
            }
        }
        registry.register(std::sync::Arc::new(
            crate::runtime::saga_compensators::ManualReviewCompensator::new("clickhouse"),
        ));
        std::sync::Arc::new(registry)
    }

    pub fn config(&self) -> &UdbConfig {
        &self.config
    }

    pub fn circuit_breaker_allows(&self, backend: &str, instance: Option<&str>) -> bool {
        let key = circuit_key(backend, instance);
        let mut breakers = self
            .circuit_breakers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(state) = breakers.get_mut(&key) else {
            return true;
        };
        if let Some(opened_until) = state.opened_until {
            if Instant::now() < opened_until {
                return false;
            }
            state.opened_until = None;
            state.failures = 0;
        }
        true
    }

    pub fn record_backend_result(&self, backend: &str, instance: Option<&str>, ok: bool) {
        let key = circuit_key(backend, instance);
        let mut breakers = self
            .circuit_breakers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if ok {
            breakers.remove(&key);
            return;
        }
        let state = breakers.entry(key).or_default();
        state.failures = state.failures.saturating_add(1);
        if state.failures >= self.config.circuit_breaker.failure_threshold.max(1) {
            state.opened_until = Some(
                Instant::now()
                    + Duration::from_secs(self.config.circuit_breaker.cooldown_secs.max(1)),
            );
        }
    }

    pub fn circuit_breaker_snapshots(&self) -> Vec<CircuitBreakerSnapshot> {
        let breakers = self
            .circuit_breakers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        breakers
            .iter()
            .map(|(key, state)| {
                let (backend, instance) = key
                    .split_once(':')
                    .map(|(backend, instance)| (backend.to_string(), instance.to_string()))
                    .unwrap_or_else(|| (key.clone(), "default".to_string()));
                let open = state
                    .opened_until
                    .map(|deadline| Instant::now() < deadline)
                    .unwrap_or(false);
                let opened_until_unix_ms = if open {
                    unix_millis()
                        + state
                            .opened_until
                            .map(|deadline| {
                                deadline
                                    .saturating_duration_since(Instant::now())
                                    .as_millis() as i64
                            })
                            .unwrap_or_default()
                } else {
                    0
                };
                CircuitBreakerSnapshot {
                    backend,
                    instance,
                    failure_count: state.failures,
                    open,
                    opened_until_unix_ms,
                }
            })
            .collect()
    }

    pub fn cache_metrics_text(&self) -> String {
        let snapshot = self.cache_metrics_snapshot();
        format!(
            "# TYPE udb_cache_hit_total counter\nudb_cache_hit_total {}\n\
             # TYPE udb_cache_miss_total counter\nudb_cache_miss_total {}\n\
             # TYPE udb_cache_invalidation_total counter\nudb_cache_invalidation_total {}\n",
            snapshot.udb_cache_hit_total,
            snapshot.udb_cache_miss_total,
            snapshot.udb_cache_invalidation_total
        )
    }

    pub fn encryption_metrics_snapshot(&self) -> EncryptionMetricSnapshot {
        self.encryption_metrics.snapshot()
    }

    pub fn encryption_metrics_text(&self) -> String {
        let snapshot = self.encryption_metrics_snapshot();
        format!(
            "# TYPE udb_encryption_ops_total counter\n\
             udb_encryption_ops_total{{op=\"encrypt\",status=\"ok\"}} {}\n\
             udb_encryption_ops_total{{op=\"encrypt\",status=\"error\"}} {}\n\
             udb_encryption_ops_total{{op=\"decrypt\",status=\"ok\"}} {}\n\
             udb_encryption_ops_total{{op=\"decrypt\",status=\"error\"}} {}\n",
            snapshot.encrypt_ok,
            snapshot.encrypt_error,
            snapshot.decrypt_ok,
            snapshot.decrypt_error
        )
    }

    pub fn pg_pool_metrics_text(&self) -> String {
        let (active, idle) = self
            .pg_pool
            .as_ref()
            .map(|pool| {
                let size = pool.size();
                let idle = pool.num_idle() as u32;
                (size.saturating_sub(idle), idle)
            })
            .unwrap_or((0, 0));
        let mut out = format!(
            "# TYPE udb_pg_pool_active_connections gauge\nudb_pg_pool_active_connections {}\n\
             # TYPE udb_pg_pool_idle_connections gauge\nudb_pg_pool_idle_connections {}\n",
            active, idle
        );
        out.push_str("# TYPE udb_pg_pool_instance_active_connections gauge\n");
        out.push_str("# TYPE udb_pg_pool_instance_idle_connections gauge\n");
        for (name, pool) in &self.pg_instances {
            let size = pool.size();
            let idle = pool.num_idle() as u32;
            let active = size.saturating_sub(idle);
            out.push_str(&format!(
                "udb_pg_pool_instance_active_connections{{instance=\"{}\"}} {}\n\
                 udb_pg_pool_instance_idle_connections{{instance=\"{}\"}} {}\n",
                name, active, name, idle
            ));
        }
        out.push_str("# TYPE udb_backend_instance_connected gauge\n");
        for instance in &self.backend_instances {
            out.push_str(&format!(
                "udb_backend_instance_connected{{backend=\"{}\",instance=\"{}\",role=\"{}\"}} {}\n",
                instance.backend,
                instance.name,
                instance.role,
                if instance.connected { 1 } else { 0 }
            ));
        }
        out.push_str(&self.pg_replicas.metrics_text());
        out.push_str(&self.connections.metrics_text());
        out
    }

    pub fn postgres_configured(&self) -> bool {
        self.pg_pool.is_some()
    }

    pub fn pg_pool_clone(&self) -> Option<PgPool> {
        self.connections
            .lease_postgres("primary")
            .map(|lease| lease.into_inner())
            .or_else(|| self.pg_pool.clone())
    }

    /// NW1-3: clone of the default `SystemStores` trait object.
    /// NW1 step 3+ call sites use this instead of `pg_pool_clone()`
    /// so the same code path works against any canonical store.
    pub fn default_system_stores_clone(
        &self,
    ) -> Option<std::sync::Arc<dyn crate::runtime::canonical_store::SystemStores>> {
        self.default_system_stores()
    }

    /// Returns the replica pool when configured, otherwise falls back to the
    /// primary pool.  Used for read-only SELECT queries (GAP 2).\
    pub fn pg_read_pool_clone(&self) -> Option<PgPool> {
        self.pg_replicas
            .choose_pool()
            .or_else(|| self.pg_pool.clone())
    }

    pub fn pg_read_pool_for_context(&self, context: &RequestContext) -> Option<PgPool> {
        self.pg_read_pool_for_context_checked(context).ok()
    }

    pub fn pg_read_pool_for_context_checked(
        &self,
        context: &RequestContext,
    ) -> Result<PgPool, tonic::Status> {
        let target_is_postgres = context.target_backend.trim().is_empty()
            || matches!(
                context.target_backend.to_ascii_lowercase().as_str(),
                "postgres" | "pg" | "postgresql"
            );
        if target_is_postgres && !context.target_instance.trim().is_empty() {
            self.ensure_backend_instance_name_allowed_for_project(
                &["postgres"],
                context.target_instance.trim(),
                &context.project_id,
            )?;
        }
        if read_fence_requires_primary(context) {
            return self
                .pg_pool
                .clone()
                .or_else(|| self.pg_instances.get("primary").cloned())
                .ok_or_else(|| tonic::Status::unavailable("PostgreSQL backend is not configured"));
        }
        if target_is_postgres {
            let target_instance = context.target_instance.trim();
            if !target_instance.is_empty() {
                if let Some(pool) = self
                    .connections
                    .lease_postgres(target_instance)
                    .map(|lease| lease.into_inner())
                    .or_else(|| self.pg_instances.get(target_instance).cloned())
                {
                    return Ok(pool);
                }
            }
        }
        if context.requires_primary_read() {
            return self
                .pg_pool
                .clone()
                .ok_or_else(|| tonic::Status::unavailable("PostgreSQL backend is not configured"));
        }
        if matches!(
            context.routing_policy.to_ascii_lowercase().as_str(),
            "primary" | "write" | "strong"
        ) {
            return self
                .pg_pool
                .clone()
                .ok_or_else(|| tonic::Status::unavailable("PostgreSQL backend is not configured"));
        }
        self.pg_replicas
            .choose_pool_with_max_lag(context.replica_lag_override())
            .or_else(|| self.pg_pool.clone())
            .ok_or_else(|| tonic::Status::unavailable("PostgreSQL backend is not configured"))
    }

    pub async fn enforce_read_fence(
        &self,
        context: &RequestContext,
        _pool: &PgPool,
        backend_label: &str,
        instance_label: &str,
    ) -> Result<(), tonic::Status> {
        if context.read_fence_json.trim().is_empty() {
            return Ok(());
        }
        let mut consistency = crate::runtime::consistency::ConsistencyPolicy::from_request_context(
            &context.consistency,
            context.max_replica_lag_ms,
            context.primary_read,
            context.eventual_consistency_allowed,
        );
        let fence = serde_json::from_str::<crate::runtime::consistency::ReadFence>(
            &context.read_fence_json,
        )
        .map_err(|err| {
            tonic::Status::invalid_argument(format!("invalid read_fence_json: {err}"))
        })?;
        consistency = consistency.with_fence(fence);
        if consistency.fence.is_empty() || !consistency.mode.honours_fence() {
            return Ok(());
        }

        // NW1-3e: route the fence through the canonical store
        // registry. The `_pool` parameter is retained on the API for
        // backward compatibility with the legacy call signature, but
        // the wait now happens against `Arc<dyn SystemStores>`. A
        // deployment without any registered canonical store
        // (slim/test) treats the fence as cleared — same effect the
        // pre-NW1-3e code path had when the PG pool query returned
        // an error.
        let Some(store) = self.default_system_stores_clone() else {
            return Ok(());
        };
        match crate::runtime::consistency_fence::wait_for_fence(
            store.as_ref(),
            &consistency.fence,
            backend_label,
            instance_label,
        )
        .await
        {
            crate::runtime::consistency_fence::FenceOutcome::Cleared => Ok(()),
            crate::runtime::consistency_fence::FenceOutcome::Stale(warning) => {
                tracing::warn!(
                    warning = ?warning,
                    "read fence did not clear before max_wait_ms"
                );
                Err(tonic::Status::deadline_exceeded(format!(
                    "read fence did not clear: {}",
                    warning.kind_token()
                )))
            }
        }
    }

    pub fn pg_replica_snapshots(&self) -> Vec<PgReplicaSnapshot> {
        self.pg_replicas.snapshots()
    }

    pub fn pg_replica_strategy(&self) -> &'static str {
        self.pg_replicas.strategy().as_str()
    }

    pub(crate) fn choose_instance_name(&self, backend: &str, write: bool) -> Option<&str> {
        self.choose_instance_name_for_project(backend, write, "")
    }

    pub(crate) fn choose_instance_name_for_project(
        &self,
        backend: &str,
        write: bool,
        project_id: &str,
    ) -> Option<&str> {
        let candidates: Vec<_> = self
            .backend_instances
            .iter()
            .filter(|instance| {
                instance.backend == backend
                    && instance.enabled
                    && instance.connected
                    && self.circuit_breaker_allows(&instance.backend, Some(&instance.name))
                    && self.instance_matches_project(instance, project_id)
                    && if write {
                        instance.role == "write"
                            || instance.role == "read_write"
                            || instance.role == "admin"
                    } else {
                        instance.role == "read"
                            || instance.role == "read_write"
                            || instance.role == "admin"
                    }
            })
            .filter(|instance| {
                if write {
                    instance.write_weight > 0
                } else {
                    instance.read_weight > 0
                }
            })
            .collect();
        if candidates.is_empty() {
            return None;
        }
        let total_weight: u64 = candidates
            .iter()
            .map(|instance| {
                (if write {
                    instance.write_weight
                } else {
                    instance.read_weight
                }) as u64
            })
            .sum();
        if total_weight == 0 {
            return None;
        }

        let slot = {
            let key = format!("{backend}:{}", if write { "write" } else { "read" });
            let mut counters = self
                .routing_counters
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let counter = counters.entry(key).or_default();
            let slot = *counter % total_weight;
            *counter = counter.wrapping_add(1);
            slot
        };

        let mut cursor = slot;
        for instance in candidates {
            let weight = if write {
                instance.write_weight
            } else {
                instance.read_weight
            } as u64;
            if cursor < weight {
                return Some(instance.name.as_str());
            }
            cursor -= weight;
        }
        None
    }

    #[cfg(feature = "redis")]
    pub(crate) fn redis_for_instance(
        &self,
        instance: Option<&str>,
    ) -> Result<&redis::Client, tonic::Status> {
        if let Some(instance) = instance.filter(|value| !value.trim().is_empty()) {
            if !self.circuit_breaker_allows("redis", Some(instance)) {
                return Err(tonic::Status::unavailable(format!(
                    "redis instance '{instance}' circuit breaker is open"
                )));
            }
            return self
                .redis_instances
                .get(instance)
                .or_else(|| {
                    if instance == "default" {
                        self.redis.as_ref()
                    } else {
                        None
                    }
                })
                .ok_or_else(|| {
                    tonic::Status::failed_precondition(format!(
                        "redis instance '{instance}' is not connected"
                    ))
                });
        }
        self.choose_instance_name("redis", false)
            .and_then(|name| self.redis_instances.get(name))
            .or(self.redis.as_ref())
            .or_else(|| self.redis_instances.values().next())
            .ok_or_else(|| tonic::Status::failed_precondition("redis not configured"))
    }

    impl_instance_resolver! {
        feature = "qdrant",
        simple = qdrant_for_instance,
        project = qdrant_for_instance_for_project,
        ret = &QdrantHttpClient,
        single = qdrant,
        instances = qdrant_instances,
        allow = ["qdrant"],
        breakers = ["qdrant"],
        unlabeled = "qdrant",
        choose = ["qdrant"],
        cb_label = "qdrant",
        not_connected_label = "qdrant",
        not_configured = unavailable("Qdrant backend is not configured"),
    }

    impl_instance_resolver! {
        feature = "s3",
        simple = s3_for_instance,
        project = s3_for_instance_for_project,
        ret = &aws_sdk_s3::Client,
        single = s3,
        instances = s3_instances,
        allow = ["minio", "s3"],
        breakers = ["minio", "s3"],
        unlabeled = "s3",
        choose = ["minio", "s3"],
        cb_label = "s3/minio",
        not_connected_label = "s3/minio",
        not_configured = failed_precondition("s3/minio not configured"),
    }

    impl_instance_resolver! {
        feature = "mongodb",
        simple = mongodb_for_instance,
        project = mongodb_for_instance_for_project,
        ret = &MongoDbExecutor,
        single = mongodb,
        instances = mongodb_instances,
        allow = ["mongodb"],
        breakers = ["mongodb"],
        unlabeled = "mongodb",
        choose = ["mongodb"],
        cb_label = "mongodb",
        not_connected_label = "mongodb",
        not_configured = failed_precondition("mongodb not configured"),
    }

    impl_instance_resolver! {
        feature = "neo4j",
        simple = neo4j_for_instance,
        project = neo4j_for_instance_for_project,
        ret = &Neo4jExecutor,
        single = neo4j,
        instances = neo4j_instances,
        allow = ["neo4j"],
        breakers = ["neo4j"],
        unlabeled = "neo4j",
        choose = ["neo4j"],
        cb_label = "neo4j",
        not_connected_label = "neo4j",
        not_configured = failed_precondition("neo4j not configured"),
    }

    impl_instance_resolver! {
        feature = "clickhouse",
        simple = clickhouse_for_instance,
        project = clickhouse_for_instance_for_project,
        ret = &ClickHouseExecutor,
        single = clickhouse,
        instances = clickhouse_instances,
        allow = ["clickhouse"],
        breakers = ["clickhouse"],
        unlabeled = "clickhouse",
        choose = ["clickhouse"],
        cb_label = "clickhouse",
        not_connected_label = "clickhouse",
        not_configured = failed_precondition("clickhouse not configured"),
    }

    #[cfg(feature = "redis")]
    pub fn redis_clone(&self) -> Option<redis::Client> {
        self.redis.clone()
    }

    #[cfg(feature = "qdrant")]
    pub fn qdrant_configured(&self) -> bool {
        self.qdrant.is_some()
    }

    #[cfg(not(feature = "qdrant"))]
    pub fn qdrant_configured(&self) -> bool {
        false
    }

    #[cfg(feature = "s3")]
    pub fn s3_configured(&self) -> bool {
        self.s3.is_some()
    }

    #[cfg(not(feature = "s3"))]
    pub fn s3_configured(&self) -> bool {
        false
    }

    /// Returns the active MongoDB transport kind (`"atlas_data_api"`) or
    /// `None` when MongoDB is not configured.
    #[cfg(feature = "mongodb")]
    pub fn mongodb_transport_kind(&self) -> Option<&'static str> {
        self.mongodb.as_ref().map(|m| m.transport_kind())
    }

    #[cfg(not(feature = "mongodb"))]
    pub fn mongodb_transport_kind(&self) -> Option<&'static str> {
        None
    }

    fn project_routing_mode(&self) -> crate::runtime::project_backend_router::ProjectRoutingMode {
        crate::runtime::project_backend_router::ProjectRoutingMode::parse(
            &self.config.project_routing_mode,
        )
    }

    fn ensure_instance_matches_project(
        &self,
        instance: &RuntimeBackendInstance,
        project_id: &str,
    ) -> Result<(), tonic::Status> {
        let mode = self.project_routing_mode();
        let decision = crate::runtime::project_backend_router::evaluate_instance_for_project(
            project_id,
            &instance.labels,
            &mode,
        );
        match decision {
            crate::runtime::project_backend_router::ProjectAccessDecision::Allowed => Ok(()),
            crate::runtime::project_backend_router::ProjectAccessDecision::NotProvisioned {
                reason,
            } => Err(tonic::Status::not_found(format!(
                "backend instance '{}:{}' is not configured for project '{}': {}",
                instance.backend,
                instance.name,
                normalized_project_id(project_id).unwrap_or("default"),
                reason
            ))),
        }
    }

    fn instance_matches_project(
        &self,
        instance: &RuntimeBackendInstance,
        project_id: &str,
    ) -> bool {
        self.ensure_instance_matches_project(instance, project_id)
            .is_ok()
    }

    fn allow_unlabeled_fallback_instance_for_project(
        &self,
        backend: &str,
        instance: &str,
        project_id: &str,
    ) -> Result<(), tonic::Status> {
        let labels = std::collections::HashMap::new();
        let mode = self.project_routing_mode();
        let decision = crate::runtime::project_backend_router::evaluate_instance_for_project(
            project_id, &labels, &mode,
        );
        match decision {
            crate::runtime::project_backend_router::ProjectAccessDecision::Allowed => Ok(()),
            crate::runtime::project_backend_router::ProjectAccessDecision::NotProvisioned {
                reason,
            } => Err(tonic::Status::not_found(format!(
                "backend instance '{}:{}' is not configured for project '{}': {}",
                backend,
                instance,
                normalized_project_id(project_id).unwrap_or("default"),
                reason
            ))),
        }
    }

    fn ensure_unlabeled_default_allowed_for_project(
        &self,
        backend: &str,
        project_id: &str,
    ) -> Result<(), tonic::Status> {
        if project_id.trim().is_empty() {
            return Ok(());
        }
        self.allow_unlabeled_fallback_instance_for_project(backend, "default", project_id)
    }

    fn ensure_backend_instance_name_allowed_for_project(
        &self,
        backends: &[&str],
        instance_name: &str,
        project_id: &str,
    ) -> Result<(), tonic::Status> {
        if project_id.trim().is_empty() {
            return Ok(());
        }
        if let Some(instance) = self.backend_instances.iter().find(|candidate| {
            candidate.name == instance_name
                && backends.iter().any(|backend| candidate.backend == *backend)
        }) {
            return self.ensure_instance_matches_project(instance, project_id);
        }
        self.allow_unlabeled_fallback_instance_for_project(
            backends.first().copied().unwrap_or("backend"),
            instance_name,
            project_id,
        )
    }
}

fn normalized_project_id(project_id: &str) -> Option<&str> {
    let project_id = project_id.trim();
    (!project_id.is_empty()).then_some(project_id)
}

fn read_fence_requires_primary(context: &RequestContext) -> bool {
    if context.read_fence_json.trim().is_empty() {
        return false;
    }
    !matches!(
        context
            .consistency
            .to_ascii_lowercase()
            .replace('-', "_")
            .as_str(),
        "cache_ok"
    )
}
