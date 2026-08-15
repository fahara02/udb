//! Continuation `impl DataBrokerRuntime` block (Phase F split of core.rs).
use super::*;

fn invalid_backend_selector_status(selector: &str) -> tonic::Status {
    crate::runtime::executor_utils::invalid_argument_fields(
        format!("unknown backend '{selector}'"),
        [("backend", "must name a supported backend")],
    )
}

fn invalid_read_fence_json_status(err: impl std::fmt::Display) -> tonic::Status {
    crate::runtime::executor_utils::invalid_argument_fields(
        format!("invalid read_fence_json: {err}"),
        [("read_fence_json", "must decode as a ReadFence JSON payload")],
    )
}

fn bounded_read_refused_status(
    refused: crate::runtime::consistency::BoundedReadRefused,
) -> tonic::Status {
    crate::runtime::executor_utils::policy_status(
        "read_consistency",
        "bounded_staleness_requires_real_position",
        refused.to_string(),
    )
}

fn backend_selector_not_found_status(
    backend: impl Into<String>,
    operation: &'static str,
    schema_code: &'static str,
    message: impl Into<String>,
) -> tonic::Status {
    crate::runtime::executor_utils::schema_status(
        tonic::Code::NotFound,
        backend,
        operation,
        schema_code,
        message,
    )
}

fn backend_instance_not_configured_status(backend: &str, instance: &str) -> tonic::Status {
    backend_selector_not_found_status(
        backend,
        "resolve_backend_selector",
        "backend_instance_not_configured",
        format!("backend instance '{backend}:{instance}' is not configured"),
    )
}

fn backend_targets_not_found_status(backend: &str, selector: &str) -> tonic::Status {
    backend_selector_not_found_status(
        backend,
        "resolve_backend_targets",
        "backend_targets_not_found",
        format!("no connected backend instances matched '{selector}'"),
    )
}

fn backend_instance_project_not_configured_status(
    backend: &str,
    instance: &str,
    project_id: &str,
    reason: impl std::fmt::Display,
) -> tonic::Status {
    backend_selector_not_found_status(
        backend,
        "project_backend_routing",
        "backend_instance_project_not_configured",
        format!(
            "backend instance '{}:{}' is not configured for project '{}': {}",
            backend,
            instance,
            normalized_project_id(project_id)
                .unwrap_or(crate::runtime::catalog::DEFAULT_PROJECT_ID),
            reason
        ),
    )
}

fn postgres_backend_not_configured_status(operation: &'static str) -> tonic::Status {
    crate::runtime::executor_utils::capability_status(
        "postgres",
        operation,
        "postgres_backend",
        "PostgreSQL backend is not configured",
    )
}

fn backend_not_configured_status(
    backend: &'static str,
    operation: &'static str,
    capability_required: &'static str,
    message: &'static str,
) -> tonic::Status {
    crate::runtime::executor_utils::capability_status(
        backend,
        operation,
        capability_required,
        message,
    )
}

fn backend_instance_not_connected_status(
    backend: impl Into<String>,
    message: impl Into<String>,
) -> tonic::Status {
    crate::runtime::executor_utils::capability_status(
        backend,
        "instance_resolver",
        "backend_instance_connected",
        message,
    )
}

fn backend_instance_disabled_status(
    backend: impl Into<String>,
    message: impl Into<String>,
) -> tonic::Status {
    crate::runtime::executor_utils::capability_status(
        backend,
        "instance_resolver",
        "backend_instance_enabled",
        message,
    )
}

fn backend_executor_not_registered_status(
    backend: impl Into<String>,
    message: impl Into<String>,
) -> tonic::Status {
    crate::runtime::executor_utils::capability_status(
        backend,
        "executor_registry",
        "backend_executor_registered",
        message,
    )
}

fn backend_executor_not_connected_status(
    backend: impl Into<String>,
    message: impl Into<String>,
) -> tonic::Status {
    crate::runtime::executor_utils::capability_status(
        backend,
        "executor_registry",
        "backend_executor_connected",
        message,
    )
}

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
/// display labels, and the final not-configured `Status`.
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
        not_configured = $not_configured:expr $(,)?
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
                    return Err(crate::runtime::executor_utils::retryable_status(
                        $cb_label,
                        "circuit_breaker_open",
                        crate::runtime::executor_utils::HTTP_RETRYABLE_BACKOFF_MS,
                        format!("{} instance '{}' circuit breaker is open", $cb_label, instance),
                    ));
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
                        backend_instance_not_connected_status(
                            $cb_label,
                            format!("{} instance '{}' is not connected", $nc_label, instance),
                        )
                    });
            }
            self.ensure_unlabeled_default_allowed_for_project($unlabeled, project_id)?;
            None
                $(.or_else(|| self.choose_instance_name_for_project($choose, false, project_id)))+
                .and_then(|name| self.$instances.get(name))
                .or(self.$single.as_ref())
                .or_else(|| self.$instances.values().next())
                .ok_or_else(|| $not_configured)
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

    /// Return only backend instances whose canonical project-routing policy
    /// permits `project_id`. This is the read-only discovery counterpart of the
    /// dispatch resolvers: project-scoped capability surfaces must not disclose
    /// instances that are explicitly labelled for another project.
    pub(crate) fn backend_instances_for_project(
        &self,
        project_id: &str,
    ) -> Vec<&RuntimeBackendInstance> {
        self.backend_instances
            .iter()
            .filter(|instance| self.instance_matches_project(instance, project_id))
            .collect()
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
                backend_executor_not_registered_status(
                    resolved.backend.clone(),
                    format!(
                        "backend executor '{}:{}' is not registered",
                        resolved.backend,
                        resolved
                            .instance
                            .as_deref()
                            .unwrap_or(crate::runtime::catalog::DEFAULT_PROJECT_ID)
                    ),
                )
            })?;
        if !registration.connected {
            return Err(backend_executor_not_connected_status(
                registration.backend.clone(),
                format!(
                    "backend executor '{}:{}' is registered but not connected",
                    registration.backend,
                    registration
                        .instance
                        .as_deref()
                        .unwrap_or(crate::runtime::catalog::DEFAULT_PROJECT_ID)
                ),
            ));
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
                && self
                    .executor_registry
                    .get(&instance.backend, Some(&instance.name))
                    .filter(|registration| registration.connected)
                    .is_some()
                && !names.contains(&instance.backend)
            {
                names.push(instance.backend.clone());
            }
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
                    backend_instance_not_connected_status(
                        "postgres",
                        format!("postgres instance '{instance}' is not connected"),
                    )
                });
        }
        self.pg_pool
            .as_ref()
            .or_else(|| self.pg_instances.get("primary"))
            .or_else(|| self.pg_instances.values().next())
            .ok_or_else(|| {
                backend_not_configured_status(
                    "postgres",
                    "pool_lookup",
                    "postgres_backend",
                    "PostgreSQL is not configured",
                )
            })
    }

    /// Resolve one exact project-authorized PostgreSQL write target.
    ///
    /// `expected_instance` is supplied when replaying deferred work. In that
    /// mode the persisted alias is revalidated rather than load-balanced onto a
    /// different instance. The returned provenance binds the routing policy to
    /// a physical PostgreSQL identity without persisting a DSN or credential.
    pub(crate) async fn project_postgres_write_target(
        &self,
        project_id: &str,
        expected_instance: Option<&str>,
    ) -> Result<ProjectPostgresWriteTarget, tonic::Status> {
        use sha2::{Digest, Sha256};

        let project_id = project_id.trim();
        if project_id.is_empty() {
            return Err(crate::runtime::executor_utils::invalid_argument_fields(
                "project PostgreSQL write routing requires an explicit project_id",
                [("project_id", "must identify an explicit project")],
            ));
        }

        let postgres_instances: Vec<&RuntimeBackendInstance> = self
            .backend_instances
            .iter()
            .filter(|instance| instance.backend == "postgres")
            .collect();
        let expected_instance = expected_instance
            .map(str::trim)
            .filter(|instance| !instance.is_empty());
        let eligible_instances: Vec<&RuntimeBackendInstance> = postgres_instances
            .iter()
            .copied()
            .filter(|instance| {
                instance.enabled
                    && instance.configured
                    && instance.connected
                    && instance.healthy
                    && instance.write_weight > 0
                    && matches!(instance.role.as_str(), "write" | "read_write" | "admin")
                    && self.circuit_breaker_allows("postgres", Some(&instance.name))
                    && self.instance_matches_project(instance, project_id)
            })
            .collect();
        let selected = if let Some(expected) = expected_instance {
            expected.to_string()
        } else if eligible_instances.len() == 1 {
            eligible_instances[0].name.clone()
        } else if eligible_instances.len() > 1 {
            return Err(crate::runtime::executor_utils::schema_status(
                tonic::Code::FailedPrecondition,
                "postgres",
                "project_write_authority",
                "project_write_authority_ambiguous",
                format!(
                    "project '{project_id}' has multiple eligible PostgreSQL write instances; schema authority requires one explicit canonical owner"
                ),
            ));
        } else if postgres_instances.is_empty() {
            self.ensure_unlabeled_default_allowed_for_project("postgres", project_id)?;
            "primary".to_string()
        } else {
            return Err(backend_instance_not_connected_status(
                "postgres",
                format!(
                    "no enabled, healthy, write-capable PostgreSQL instance is routed to project '{project_id}'"
                ),
            ));
        };

        let registration = postgres_instances
            .iter()
            .copied()
            .find(|instance| instance.name == selected);
        let (role, dsn_env, labels) = if let Some(instance) = registration {
            self.ensure_instance_matches_project(instance, project_id)?;
            if !instance.enabled
                || !instance.configured
                || !instance.connected
                || !instance.healthy
                || instance.write_weight == 0
                || !matches!(instance.role.as_str(), "write" | "read_write" | "admin")
            {
                return Err(backend_instance_not_connected_status(
                    "postgres",
                    format!(
                        "postgres instance '{}' is not an enabled, healthy write target",
                        instance.name
                    ),
                ));
            }
            if !self.circuit_breaker_allows("postgres", Some(&instance.name)) {
                return Err(crate::runtime::executor_utils::retryable_status(
                    "postgres",
                    "circuit_breaker_open",
                    crate::runtime::executor_utils::HTTP_RETRYABLE_BACKOFF_MS,
                    format!("postgres instance '{}' circuit breaker is open", instance.name),
                ));
            }
            (
                instance.role.clone(),
                instance.dsn_env.clone().unwrap_or_default(),
                instance
                    .labels
                    .iter()
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect::<std::collections::BTreeMap<_, _>>(),
            )
        } else {
            if !postgres_instances.is_empty() || selected != "primary" {
                return Err(backend_instance_not_configured_status(
                    "postgres",
                    &selected,
                ));
            }
            self.ensure_unlabeled_default_allowed_for_project("postgres", project_id)?;
            (
                "legacy_primary".to_string(),
                String::new(),
                std::collections::BTreeMap::new(),
            )
        };

        let pool = self.pg_pool_for_instance(Some(&selected))?.clone();
        let (database, user, server_address, server_port): (
            String,
            String,
            Option<String>,
            Option<i32>,
        ) = sqlx::query_as(
            "SELECT current_database()::TEXT, current_user::TEXT,
                    inet_server_addr()::TEXT, inet_server_port()",
        )
        .fetch_one(&pool)
        .await
        .map_err(|err| {
            crate::runtime::executor_utils::retryable_status(
                "postgres",
                "project_write_target_provenance_unavailable",
                crate::runtime::executor_utils::HTTP_RETRYABLE_BACKOFF_MS,
                format!(
                    "failed to prove PostgreSQL write target '{}' for project '{}': {err}",
                    selected, project_id
                ),
            )
        })?;
        let labels_json = serde_json::to_string(&labels).unwrap_or_default();
        let routing_mode = self.config.project_routing_mode.trim().to_ascii_lowercase();
        let server_address = server_address.unwrap_or_default();
        let server_port = server_port.unwrap_or_default().to_string();
        let mut hasher = Sha256::new();
        for part in [
            "UDB_PROJECT_POSTGRES_TARGET_V1",
            project_id,
            "postgres",
            selected.as_str(),
            role.as_str(),
            dsn_env.as_str(),
            labels_json.as_str(),
            routing_mode.as_str(),
            database.as_str(),
            user.as_str(),
            server_address.as_str(),
            server_port.as_str(),
        ] {
            hasher.update((part.len() as u64).to_be_bytes());
            hasher.update(part.as_bytes());
        }

        Ok(ProjectPostgresWriteTarget {
            instance: selected,
            pool,
            provenance_sha256: format!("{:x}", hasher.finalize()),
        })
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
            .ok_or_else(|| invalid_backend_selector_status(selector))?;
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
                return Err(backend_instance_not_configured_status(
                    &backend,
                    instance_name,
                ));
            };
            self.ensure_instance_matches_project(runtime_instance, project_id)?;
            if !runtime_instance.enabled {
                return Err(backend_instance_disabled_status(
                    backend.clone(),
                    format!(
                        "backend instance '{}:{}' is disabled",
                        backend, instance_name
                    ),
                ));
            }
            if !runtime_instance.connected {
                return Err(backend_instance_not_connected_status(
                    backend.clone(),
                    format!(
                        "backend instance '{}:{}' is configured but not connected",
                        backend, instance_name
                    ),
                ));
            }
            if !self.circuit_breaker_allows(&backend, Some(instance_name)) {
                return Err(crate::runtime::executor_utils::retryable_status(
                    backend.clone(),
                    "circuit_breaker_open",
                    crate::runtime::executor_utils::HTTP_RETRYABLE_BACKOFF_MS,
                    format!(
                        "backend instance '{}:{}' circuit breaker is open",
                        backend, instance_name
                    ),
                ));
            }
        }
        Ok(ResolvedBackendSelector { backend, instance })
    }

    fn resolve_projection_target_for_project(
        &self,
        backend: &str,
        instance: Option<&str>,
        project_id: &str,
        write: bool,
    ) -> Result<ResolvedBackendSelector, tonic::Status> {
        let project_id = project_id.trim();
        let operation = if write {
            "projection_write"
        } else {
            "projection_read"
        };
        if project_id.is_empty() {
            return Err(crate::runtime::executor_utils::schema_status(
                tonic::Code::FailedPrecondition,
                backend,
                operation,
                "projection_project_required",
                "projection routing requires a non-empty project_id",
            ));
        }
        if let Some(instance) = instance.filter(|value| !value.trim().is_empty()) {
            let resolved = self.resolve_backend_selector_for_project(
                &format!("{backend}:{}", instance.trim()),
                project_id,
            )?;
            let registration = self.backend_instances.iter().find(|candidate| {
                candidate.backend == resolved.backend
                    && resolved.instance.as_deref() == Some(candidate.name.as_str())
            });
            if let Some(registration) = registration {
                let role_allowed = if write {
                    matches!(registration.role.as_str(), "write" | "read_write" | "admin")
                        && registration.write_weight > 0
                } else {
                    matches!(registration.role.as_str(), "read" | "read_write" | "admin")
                        && registration.read_weight > 0
                };
                if !registration.configured || !registration.healthy || !role_allowed {
                    return Err(backend_instance_not_connected_status(
                        resolved.backend,
                        format!(
                            "backend instance '{}:{}' is not a configured, healthy {} target",
                            registration.backend,
                            registration.name,
                            if write { "write" } else { "read" }
                        ),
                    ));
                }
            } else if project_id != crate::runtime::catalog::DEFAULT_PROJECT_ID {
                return Err(backend_selector_not_found_status(
                    resolved.backend,
                    operation,
                    "project_backend_instance_missing",
                    format!(
                        "backend instance '{}:{}' has no explicit project binding for '{project_id}'",
                        backend,
                        instance.trim()
                    ),
                ));
            }
            return Ok(resolved);
        }

        let base = self.resolve_backend_selector_for_project(backend, project_id)?;
        let selected = if base.backend == "s3" {
            self.choose_instance_name_for_project("minio", write, project_id)
                .or_else(|| self.choose_instance_name_for_project("s3", write, project_id))
        } else {
            self.choose_instance_name_for_project(&base.backend, write, project_id)
        };
        if let Some(selected) = selected {
            return self.resolve_projection_target_for_project(
                &base.backend,
                Some(selected),
                project_id,
                write,
            );
        }
        if project_id != crate::runtime::catalog::DEFAULT_PROJECT_ID {
            return Err(backend_selector_not_found_status(
                base.backend,
                operation,
                "project_backend_instance_missing",
                format!(
                    "no {}-capable backend instance is bound to project '{project_id}'",
                    if write { "write" } else { "read" }
                ),
            ));
        }
        Ok(base)
    }

    /// Resolve the exact write target for a queued projection task. Customer
    /// projects must resolve to a registered instance; they never fall back to
    /// a process-wide default client that has no project binding.
    pub(crate) fn resolve_projection_write_target_for_project(
        &self,
        backend: &str,
        instance: Option<&str>,
        project_id: &str,
    ) -> Result<ResolvedBackendSelector, tonic::Status> {
        self.resolve_projection_target_for_project(backend, instance, project_id, true)
    }

    /// Resolve the exact read target for a projection drift scan. As with
    /// writes, a non-default project cannot use an unlabeled process default.
    pub(crate) fn resolve_projection_read_target_for_project(
        &self,
        backend: &str,
        instance: Option<&str>,
        project_id: &str,
    ) -> Result<ResolvedBackendSelector, tonic::Status> {
        self.resolve_projection_target_for_project(backend, instance, project_id, false)
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
            .ok_or_else(|| invalid_backend_selector_status(selector))?;
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
            return Err(backend_targets_not_found_status(&backend, selector));
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
                    .unwrap_or_else(|| {
                        (
                            key.clone(),
                            crate::runtime::catalog::DEFAULT_PROJECT_ID.to_string(),
                        )
                    });
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
                .ok_or_else(|| postgres_backend_not_configured_status("read_fence_primary"));
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
                .ok_or_else(|| postgres_backend_not_configured_status("primary_read"));
        }
        if matches!(
            context.routing_policy.to_ascii_lowercase().as_str(),
            "primary" | "write" | "strong"
        ) {
            return self
                .pg_pool
                .clone()
                .ok_or_else(|| postgres_backend_not_configured_status("routed_read"));
        }
        // 6.4: consult the typed consistency routing decision for the one case
        // the legacy replica fallback gets WRONG — an explicit bounded-staleness
        // read against a backend that mints no real replication-position token.
        // Such a read cannot be fenced honestly (the fence would be a vacuous
        // wall-clock fence), so it is REFUSED with a typed `FailedPrecondition`
        // rather than silently served a stale replica. `Strong` / primary-forced
        // / `ReadYourWrites` reads already returned the primary above; an empty
        // consistency hint and the eventual/projection/cache modes stay
        // replica-eligible exactly as before. The full `ReplicaBounded` LSN
        // fence (which needs an `await` on the replica position) is honoured by
        // the async `pg_read_pool_routed` selector below.
        if !context.consistency.trim().is_empty() {
            let backend_label = if context.target_backend.trim().is_empty() {
                "postgres"
            } else {
                context.target_backend.trim()
            };
            let policy = crate::runtime::consistency::ConsistencyPolicy::from_request_context(
                &context.consistency,
                context.max_replica_lag_ms,
                context.primary_read,
                context.eventual_consistency_allowed,
            );
            if let crate::runtime::consistency::ReadRouting::RefusedBounded(refused) =
                policy.route_read(false, backend_label)
            {
                return Err(bounded_read_refused_status(refused));
            }
        }
        self.pg_replicas
            .choose_pool_with_max_lag(context.replica_lag_override())
            .or_else(|| self.pg_pool.clone())
            .ok_or_else(|| postgres_backend_not_configured_status("replica_or_primary_read"))
    }

    /// Async, fully-enforced read-pool selector — the budget- and
    /// bounded-staleness-aware counterpart of
    /// [`Self::pg_read_pool_for_context_checked`].
    ///
    /// The synchronous selector runs inside sync pool-resolution call sites and
    /// can therefore only honour the parts of the routing decision that need no
    /// `await`: it surfaces the typed refusal for a bounded read against a
    /// wall-clock backend but otherwise falls back to lag-bounded replica
    /// selection. This async selector honours the WHOLE
    /// [`crate::runtime::consistency::ConsistencyPolicy::route_read`] decision
    /// plus the per-tenant connection budget:
    ///
    /// - **6.3 per-tenant connection budget** — enforced via
    ///   [`crate::runtime::connection_manager::ConnectionManager::lease_postgres_for_tenant`]
    ///   / `acquire_tenant_connection` BEFORE a pooled connection is handed out;
    ///   the permit (and, for a registered instance pool, the lease accounting)
    ///   is held inside [`RoutedReadPool`] for the borrowed connection's
    ///   lifetime. An unbudgeted tenant is unlimited (pre-tiering behavior).
    /// - **6.4 REPLICA_BOUNDED fence** — a `ReplicaBounded` decision WAITS on
    ///   the chosen replica's real WAL replay position past the caller's write
    ///   LSN ([`crate::runtime::replica::PgReplicaManager::choose_bounded_replica`]),
    ///   failing over to the primary on timeout; `RefusedBounded` returns the
    ///   typed error; and writes (`is_write`) always route to the primary.
    pub async fn pg_read_pool_routed(
        &self,
        context: &RequestContext,
        is_write: bool,
    ) -> Result<RoutedReadPool, tonic::Status> {
        use crate::runtime::consistency::{ConsistencyPolicy, ReadFence, ReadRouting};
        use crate::runtime::replica::BoundedReplicaRead;

        let tenant = {
            let tenant = context.tenant_id.trim();
            (!tenant.is_empty()).then_some(tenant)
        };
        let backend_label = if context.target_backend.trim().is_empty() {
            "postgres"
        } else {
            context.target_backend.trim()
        };
        let target_is_postgres = matches!(
            backend_label.to_ascii_lowercase().as_str(),
            "postgres" | "pg" | "postgresql"
        );
        if target_is_postgres && !context.target_instance.trim().is_empty() {
            self.ensure_backend_instance_name_allowed_for_project(
                &["postgres"],
                context.target_instance.trim(),
                &context.project_id,
            )?;
        }
        let mut policy = ConsistencyPolicy::from_request_context(
            &context.consistency,
            context.max_replica_lag_ms,
            context.primary_read,
            context.eventual_consistency_allowed,
        );
        if !context.read_fence_json.trim().is_empty()
            && let Ok(fence) = serde_json::from_str::<ReadFence>(&context.read_fence_json)
        {
            policy = policy.with_fence(fence);
        }

        match policy.route_read(is_write, backend_label) {
            ReadRouting::RefusedBounded(refused) => Err(bounded_read_refused_status(refused)),
            ReadRouting::Primary => self.routed_primary_pool(context, tenant).await,
            ReadRouting::ReplicaUnfenced => match self.pg_replicas.choose_pool() {
                Some(pool) => self.routed_replica_pool(pool, tenant).await,
                None => self.routed_primary_pool(context, tenant).await,
            },
            ReadRouting::ReplicaBounded {
                max_staleness_ms,
                min_lsn,
            } => {
                let budget = std::time::Duration::from_millis(max_staleness_ms);
                match self
                    .pg_replicas
                    .choose_bounded_replica(min_lsn.as_deref(), budget)
                    .await
                {
                    BoundedReplicaRead::Replica(pool) => {
                        self.routed_replica_pool(pool, tenant).await
                    }
                    // 6.4: the chosen replica couldn't be proven fresh within
                    // the staleness budget (no eligible replica, or its real
                    // WAL replay position didn't reach the fence LSN in time).
                    // Serve from the primary, but ATTACH the carried
                    // `StaleReadWarning` so the failed-over read is never
                    // returned silently.
                    BoundedReplicaRead::FailoverToPrimary(warning) => {
                        let mut routed = self.routed_primary_pool(context, tenant).await?;
                        routed.warning = Some(warning);
                        Ok(routed)
                    }
                }
            }
        }
    }

    /// Primary-pool branch of [`Self::pg_read_pool_routed`]: prefer the
    /// budget-enforced lease against the registered primary instance (so both
    /// the per-tenant budget AND the lease accounting apply); fall back to the
    /// canonical primary handle under a bare budget permit when that instance is
    /// not registered with the `ConnectionManager`.
    async fn routed_primary_pool(
        &self,
        context: &RequestContext,
        tenant: Option<&str>,
    ) -> Result<RoutedReadPool, tonic::Status> {
        let instance = {
            let target = context.target_instance.trim();
            if target.is_empty() { "primary" } else { target }
        };
        if let Some(lease) = self
            .connections
            .lease_postgres_for_tenant(instance, tenant)
            .await?
        {
            return Ok(RoutedReadPool {
                pool: lease.pool(),
                _lease: Some(lease),
                _permit: None,
                warning: None,
            });
        }
        let permit = self.connections.acquire_tenant_connection(tenant).await?;
        let pool = self
            .pg_pool
            .clone()
            .ok_or_else(|| postgres_backend_not_configured_status("routed_primary_pool"))?;
        Ok(RoutedReadPool {
            pool,
            _lease: None,
            _permit: permit,
            warning: None,
        })
    }

    /// Replica-pool branch of [`Self::pg_read_pool_routed`]: replica pools live
    /// in the `PgReplicaManager`, not the `ConnectionManager`, so the per-tenant
    /// budget is enforced with a bare permit held for the borrowed connection's
    /// lifetime.
    async fn routed_replica_pool(
        &self,
        pool: PgPool,
        tenant: Option<&str>,
    ) -> Result<RoutedReadPool, tonic::Status> {
        let permit = self.connections.acquire_tenant_connection(tenant).await?;
        Ok(RoutedReadPool {
            pool,
            _lease: None,
            _permit: permit,
            warning: None,
        })
    }

    pub async fn enforce_read_fence(
        &self,
        context: &RequestContext,
        backend_label: &str,
        instance_label: &str,
    ) -> Result<Option<crate::runtime::consistency::StaleReadWarning>, tonic::Status> {
        use crate::runtime::consistency::{ConsistencyMode, StaleReadWarning};
        if context.read_fence_json.trim().is_empty() {
            return Ok(None);
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
        .map_err(invalid_read_fence_json_status)?;
        consistency = consistency.with_fence(fence);
        if consistency.fence.is_empty() || !consistency.mode.honours_fence() {
            return Ok(None);
        }

        // NW1-3e: route the fence through the canonical store registry; the
        // wait happens against `Arc<dyn SystemStores>` and is backend-agnostic
        // (no PG pool required), so every read entrypoint can call this.
        //
        // 03.3.6: the no-store early return is a DELIBERATE fail-open for
        // storeless deployments (slim/test builds with no registered canonical
        // store). It is NOT a bug — a fence cannot be honoured without a store
        // to wait against, so we treat it as cleared (`Ok(None)`) rather than
        // hard-failing the read. Do not change this to an error.
        let Some(store) = self.default_system_stores_clone() else {
            return Ok(None);
        };
        match crate::runtime::consistency_fence::wait_for_fence(
            store.as_ref(),
            &consistency.fence,
            backend_label,
            instance_label,
        )
        .await
        {
            crate::runtime::consistency_fence::FenceOutcome::Cleared => Ok(None),
            // 03.2.1.1: component- and mode-aware stale handling. The matrix is
            // intentionally narrow — only Eventual/BoundedStaleness (and
            // ProjectionOk for non-own-write staleness) serve stale data with a
            // warning; Strong/ReadYourWrites always hard-fail, and ProjectionOk
            // hard-fails on its OWN un-projected write (ProjectionMissing).
            crate::runtime::consistency_fence::FenceOutcome::Stale(warning) => {
                let hard_fail = |warning: &StaleReadWarning| {
                    tracing::warn!(
                        warning = ?warning,
                        "read fence did not clear before max_wait_ms"
                    );
                    crate::runtime::executor_utils::deadline_exceeded_status(
                        backend_label,
                        format!("read_fence_{}", warning.kind_token()),
                        crate::runtime::executor_utils::HTTP_RETRYABLE_BACKOFF_MS,
                        format!("read fence did not clear: {}", warning.kind_token()),
                    )
                };
                match consistency.mode {
                    ConsistencyMode::Strong | ConsistencyMode::ReadYourWrites => {
                        Err(hard_fail(&warning))
                    }
                    ConsistencyMode::ProjectionOk => {
                        if matches!(warning, StaleReadWarning::ProjectionMissing { .. }) {
                            Err(hard_fail(&warning))
                        } else {
                            Ok(Some(warning))
                        }
                    }
                    ConsistencyMode::Eventual
                    | ConsistencyMode::BoundedStaleness
                    | ConsistencyMode::ReplicaBounded => Ok(Some(warning)),
                    // Unreachable: CacheOk was already filtered by
                    // `!honours_fence()` above. Returned as cleared defensively.
                    ConsistencyMode::CacheOk => Ok(None),
                }
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
                return Err(crate::runtime::executor_utils::retryable_status(
                    "redis",
                    "circuit_breaker_open",
                    crate::runtime::executor_utils::HTTP_RETRYABLE_BACKOFF_MS,
                    format!("redis instance '{instance}' circuit breaker is open"),
                ));
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
                    backend_instance_not_connected_status(
                        "redis",
                        format!("redis instance '{instance}' is not connected"),
                    )
                });
        }
        self.choose_instance_name("redis", false)
            .and_then(|name| self.redis_instances.get(name))
            .or(self.redis.as_ref())
            .or_else(|| self.redis_instances.values().next())
            .ok_or_else(|| {
                backend_not_configured_status(
                    "redis",
                    "instance_resolver",
                    "redis_backend",
                    "redis not configured",
                )
            })
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
        not_configured = backend_not_configured_status(
            "qdrant",
            "instance_resolver",
            "qdrant_backend",
            "Qdrant backend is not configured",
        ),
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
        not_configured = backend_not_configured_status(
            "s3",
            "instance_resolver",
            "s3_backend",
            "s3/minio not configured"
        ),
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
        not_configured = backend_not_configured_status(
            "mongodb",
            "instance_resolver",
            "mongodb_backend",
            "mongodb not configured"
        ),
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
        not_configured = backend_not_configured_status(
            "neo4j",
            "instance_resolver",
            "neo4j_backend",
            "neo4j not configured"
        ),
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
        not_configured = backend_not_configured_status(
            "clickhouse",
            "instance_resolver",
            "clickhouse_backend",
            "clickhouse not configured"
        ),
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

    pub fn backend_transport_label(&self, kind: crate::backend::BackendKind) -> &'static str {
        match kind {
            crate::backend::BackendKind::Mongodb => self
                .mongodb_transport_kind()
                .unwrap_or_else(|| kind.transport_label()),
            _ => kind.transport_label(),
        }
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
            } => Err(backend_instance_project_not_configured_status(
                &instance.backend,
                &instance.name,
                project_id,
                reason,
            )),
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
            } => Err(backend_instance_project_not_configured_status(
                backend, instance, project_id, reason,
            )),
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
        self.allow_unlabeled_fallback_instance_for_project(
            backend,
            crate::runtime::catalog::DEFAULT_PROJECT_ID,
            project_id,
        )
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

/// RAII guard returned by [`DataBrokerRuntime::pg_read_pool_routed`]: the
/// selected read pool plus the drop guards that must outlive the borrowed
/// connection — the per-tenant DB-connection budget permit (when the tenant is
/// budgeted) and, for a read served from a `ConnectionManager`-registered
/// instance pool, the lease-accounting handle. Dropping the guard releases the
/// tenant's budget slot (and the lease count).
#[derive(Debug)]
pub struct RoutedReadPool {
    pool: PgPool,
    _lease: Option<crate::runtime::connection_manager::TenantPgLease>,
    _permit: Option<crate::runtime::connection_manager::TenantConnectionPermit>,
    /// 6.4 REPLICA_BOUNDED: set when a bounded replica read FAILED OVER to the
    /// primary because the replica couldn't be proven fresh within the
    /// staleness budget (no eligible replica, or its real WAL replay position
    /// didn't reach the fence LSN in time). The caller attaches it to the
    /// response so a failed-over read is never returned silently.
    warning: Option<crate::runtime::consistency::StaleReadWarning>,
}

impl RoutedReadPool {
    /// The pool to run the read against.
    pub fn pool(&self) -> PgPool {
        self.pool.clone()
    }

    /// The stale/failover warning to attach to the response, if the bounded
    /// read failed over to the primary. `None` for a normal (replica or
    /// primary) routing decision.
    pub fn warning(&self) -> Option<&crate::runtime::consistency::StaleReadWarning> {
        self.warning.as_ref()
    }
}

fn normalized_project_id(project_id: &str) -> Option<&str> {
    let project_id = project_id.trim();
    (!project_id.is_empty()).then_some(project_id)
}

pub(crate) fn read_fence_requires_primary(context: &RequestContext) -> bool {
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

#[cfg(test)]
mod read_fence_tests {
    use super::*;
    use crate::proto::{ErrorDetail, ErrorKind};
    use crate::runtime::consistency::ReadFence;
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;

    fn decode_detail(status: &tonic::Status) -> ErrorDetail {
        let raw = status
            .metadata()
            .get_bin(ERROR_DETAIL_METADATA_KEY)
            .expect("typed error detail trailer");
        crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
    }

    fn assert_single_field_violation(status: &tonic::Status, field: &str, description: &str) {
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert!(!detail.retryable);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, field);
        assert_eq!(detail.field_violations[0].description, description);
    }

    fn assert_capability_detail(
        status: &tonic::Status,
        backend: &str,
        operation: &str,
        capability_required: &str,
        message: &str,
    ) {
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Capability as i32);
        assert_eq!(detail.backend, backend);
        assert_eq!(detail.operation, operation);
        assert_eq!(detail.capability_required, capability_required);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
    }

    fn assert_policy_detail(
        status: &tonic::Status,
        operation: &str,
        policy_decision_id: &str,
        message: &str,
    ) {
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Policy as i32);
        assert_eq!(detail.operation, operation);
        assert_eq!(detail.policy_decision_id, policy_decision_id);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
    }

    #[test]
    fn backend_selector_validation_carries_field_violations() {
        let runtime = DataBrokerRuntime::planning_only();

        let selector_status = runtime
            .resolve_backend_selector("bogus:primary")
            .expect_err("unknown backend must fail");
        assert_eq!(selector_status.message(), "unknown backend 'bogus:primary'");
        assert_single_field_violation(&selector_status, "backend", "must name a supported backend");

        let targets_status = runtime
            .resolve_backend_targets("bogus:*", "{}")
            .expect_err("unknown target backend must fail");
        assert_eq!(targets_status.message(), "unknown backend 'bogus:*'");
        assert_single_field_violation(&targets_status, "backend", "must name a supported backend");
    }

    #[test]
    fn backend_resolver_missing_setup_carries_capability_detail() {
        let runtime = DataBrokerRuntime::planning_only();

        let pg_status = match runtime.pg_pool_for_instance(None) {
            Err(status) => status,
            Ok(_) => panic!("storeless runtime must report missing Postgres setup"),
        };
        assert_capability_detail(
            &pg_status,
            "postgres",
            "pool_lookup",
            "postgres_backend",
            "PostgreSQL is not configured",
        );

        let routed_pg_status = postgres_backend_not_configured_status("primary_read");
        assert_capability_detail(
            &routed_pg_status,
            "postgres",
            "primary_read",
            "postgres_backend",
            "PostgreSQL backend is not configured",
        );

        let redis_status = backend_not_configured_status(
            "redis",
            "instance_resolver",
            "redis_backend",
            "redis not configured",
        );
        assert_capability_detail(
            &redis_status,
            "redis",
            "instance_resolver",
            "redis_backend",
            "redis not configured",
        );
    }

    #[test]
    fn backend_resolver_connectivity_denials_carry_capability_detail() {
        let runtime = DataBrokerRuntime::planning_only();

        let pg_instance_status = runtime
            .pg_pool_for_instance(Some("replica-a"))
            .expect_err("missing named Postgres instance must fail");
        assert_capability_detail(
            &pg_instance_status,
            "postgres",
            "instance_resolver",
            "backend_instance_connected",
            "postgres instance 'replica-a' is not connected",
        );

        let disabled_status = backend_instance_disabled_status(
            "postgres",
            "backend instance 'postgres:replica-a' is disabled",
        );
        assert_capability_detail(
            &disabled_status,
            "postgres",
            "instance_resolver",
            "backend_instance_enabled",
            "backend instance 'postgres:replica-a' is disabled",
        );

        let unregistered_status = backend_executor_not_registered_status(
            "qdrant",
            "backend executor 'qdrant:default' is not registered",
        );
        assert_capability_detail(
            &unregistered_status,
            "qdrant",
            "executor_registry",
            "backend_executor_registered",
            "backend executor 'qdrant:default' is not registered",
        );

        let disconnected_status = backend_executor_not_connected_status(
            "qdrant",
            "backend executor 'qdrant:default' is registered but not connected",
        );
        assert_capability_detail(
            &disconnected_status,
            "qdrant",
            "executor_registry",
            "backend_executor_connected",
            "backend executor 'qdrant:default' is registered but not connected",
        );
    }

    #[test]
    fn bounded_read_refusal_carries_policy_detail_in_sync_selector() {
        let runtime = DataBrokerRuntime::planning_only();
        let context = RequestContext {
            target_backend: "s3".to_string(),
            consistency: "bounded_staleness".to_string(),
            max_replica_lag_ms: 50,
            ..RequestContext::default()
        };

        let status = runtime
            .pg_read_pool_for_context_checked(&context)
            .expect_err("wall-clock backend must refuse bounded-staleness reads");

        assert_policy_detail(
            &status,
            "read_consistency",
            "bounded_staleness_requires_real_position",
            "bounded-staleness read refused for backend 's3': backend mints no real replication-position token; a bounded-staleness fence on it would be a vacuous wall-clock fence",
        );
    }

    #[tokio::test]
    async fn bounded_read_refusal_carries_policy_detail_in_async_selector() {
        let runtime = DataBrokerRuntime::planning_only();
        let context = RequestContext {
            target_backend: "s3".to_string(),
            consistency: "bounded-staleness".to_string(),
            max_replica_lag_ms: 50,
            ..RequestContext::default()
        };

        let status = runtime
            .pg_read_pool_routed(&context, false)
            .await
            .expect_err("wall-clock backend must refuse bounded-staleness reads");

        assert_policy_detail(
            &status,
            "read_consistency",
            "bounded_staleness_requires_real_position",
            "bounded-staleness read refused for backend 's3': backend mints no real replication-position token; a bounded-staleness fence on it would be a vacuous wall-clock fence",
        );
    }

    #[cfg(feature = "s3")]
    #[test]
    fn s3_resolver_missing_backend_carries_capability_detail() {
        let runtime = DataBrokerRuntime::planning_only();
        let status = match runtime.s3_for_instance_for_project(None, "") {
            Err(status) => status,
            Ok(_) => panic!("storeless runtime must report missing S3 setup"),
        };

        assert_capability_detail(
            &status,
            "s3",
            "instance_resolver",
            "s3_backend",
            "s3/minio not configured",
        );
    }

    #[tokio::test]
    async fn storeless_runtime_treats_non_empty_read_fence_as_cleared() {
        let runtime = DataBrokerRuntime::planning_only();
        let fence = ReadFence {
            min_outbox_lsn: "0/16B6C50".to_string(),
            projection_task_ids: vec!["projection-task-1".to_string()],
            max_wait_ms: 1,
        };
        let context = RequestContext {
            read_fence_json: serde_json::to_string(&fence).expect("serialize fence"),
            consistency: "read_your_writes".to_string(),
            ..RequestContext::default()
        };

        let warning = runtime
            .enforce_read_fence(&context, "postgres", "default")
            .await
            .expect("storeless runtime must not hard-fail a read fence");

        assert!(
            warning.is_none(),
            "without a registered system store, a non-empty fence is deliberately treated as cleared"
        );
    }

    #[tokio::test]
    async fn malformed_read_fence_json_carries_field_violation() {
        let runtime = DataBrokerRuntime::planning_only();
        let context = RequestContext {
            read_fence_json: "{".to_string(),
            ..RequestContext::default()
        };

        let status = runtime
            .enforce_read_fence(&context, "postgres", "default")
            .await
            .expect_err("malformed fence json must fail before store access");

        assert!(
            status.message().starts_with("invalid read_fence_json:"),
            "unexpected message: {}",
            status.message()
        );
        assert_single_field_violation(
            &status,
            "read_fence_json",
            "must decode as a ReadFence JSON payload",
        );
    }
}
