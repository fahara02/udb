//! Zero-downtime config reload primitives (U16).

use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum ConfigReloadMode {
    DryRun,
    Apply,
}

impl Default for ConfigReloadMode {
    fn default() -> Self {
        Self::Apply
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ConfigReloadOptions {
    pub mode: ConfigReloadMode,
    pub reason: String,
    pub require_connected_backends: bool,
    pub rollback_on_failed_health: bool,
}

impl Default for ConfigReloadOptions {
    fn default() -> Self {
        Self {
            mode: ConfigReloadMode::Apply,
            reason: String::new(),
            require_connected_backends: false,
            rollback_on_failed_health: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Default)]
pub struct ConfigReloadReport {
    pub reload_id: String,
    pub accepted: bool,
    pub applied: bool,
    pub dry_run: bool,
    pub rolled_back: bool,
    pub reason: String,
    pub previous_generation: u64,
    pub new_generation: u64,
    pub validation_errors: Vec<String>,
    pub warnings: Vec<String>,
    pub failed_health_checks: Vec<String>,
    pub previous_client_count: usize,
    pub shadow_client_count: usize,
    pub active_client_count: usize,
    pub changed_instances: Vec<String>,
}

impl DataBrokerRuntime {
    /// Validate and optionally apply a new runtime config with shadow-client
    /// construction before the active connection manager is swapped.
    pub async fn reload_from_config(
        &mut self,
        new_config: UdbConfig,
        options: ConfigReloadOptions,
    ) -> ConfigReloadReport {
        let reload_id = Uuid::new_v4().to_string();
        let previous_generation = reload_generation(&self.config);
        let new_generation = reload_generation(&new_config);
        let previous = self.connection_snapshots();
        let validation = new_config.validate();
        if !validation.passed {
            return ConfigReloadReport {
                reload_id,
                accepted: false,
                applied: false,
                dry_run: options.mode == ConfigReloadMode::DryRun,
                rolled_back: false,
                reason: options.reason,
                previous_generation,
                new_generation,
                validation_errors: validation.errors,
                warnings: validation.warnings,
                previous_client_count: previous.len(),
                ..ConfigReloadReport::default()
            };
        }

        if options.mode == ConfigReloadMode::DryRun {
            return ConfigReloadReport {
                reload_id,
                accepted: true,
                applied: false,
                dry_run: true,
                rolled_back: false,
                reason: options.reason,
                previous_generation,
                new_generation,
                warnings: validation.warnings,
                previous_client_count: previous.len(),
                ..ConfigReloadReport::default()
            };
        }

        let shadow = match DataBrokerRuntime::try_from_config(new_config).await {
            Ok(runtime) => runtime,
            Err(err) => {
                return ConfigReloadReport {
                    reload_id,
                    accepted: false,
                    applied: false,
                    dry_run: false,
                    rolled_back: true,
                    reason: options.reason,
                    previous_generation,
                    new_generation,
                    validation_errors: vec![err],
                    warnings: validation.warnings,
                    previous_client_count: previous.len(),
                    ..ConfigReloadReport::default()
                };
            }
        };
        self.apply_shadow_runtime(shadow, options, previous, validation.warnings)
    }

    fn apply_shadow_runtime(
        &mut self,
        mut shadow: DataBrokerRuntime,
        options: ConfigReloadOptions,
        previous: Vec<crate::runtime::connection_manager::ClientSnapshot>,
        mut warnings: Vec<String>,
    ) -> ConfigReloadReport {
        let reload_id = Uuid::new_v4().to_string();
        let previous_generation = reload_generation(&self.config);
        let new_generation = reload_generation(&shadow.config);
        let failed_health_checks = shadow_failed_health_checks(&shadow);
        if options.require_connected_backends && !failed_health_checks.is_empty() {
            warnings.push("shadow runtime failed connected-backend health gate".to_string());
            return ConfigReloadReport {
                reload_id,
                accepted: true,
                applied: false,
                dry_run: false,
                rolled_back: options.rollback_on_failed_health,
                reason: options.reason,
                previous_generation,
                new_generation,
                warnings,
                failed_health_checks,
                previous_client_count: previous.len(),
                shadow_client_count: shadow.connection_snapshots().len(),
                active_client_count: self.connection_snapshots().len(),
                changed_instances: Vec::new(),
                ..ConfigReloadReport::default()
            };
        }

        let shadow_snapshots = shadow.connection_snapshots();
        let changed_instances = changed_connection_instances(&previous, &shadow_snapshots);
        let merged_connections = self.connections.clone();
        merged_connections.replace_all_from(shadow.connection_manager());
        shadow.connections = merged_connections;
        *self = shadow;
        let active_client_count = self.connection_snapshots().len();

        ConfigReloadReport {
            reload_id,
            accepted: true,
            applied: true,
            dry_run: false,
            rolled_back: false,
            reason: options.reason,
            previous_generation,
            new_generation,
            warnings,
            failed_health_checks,
            previous_client_count: previous.len(),
            shadow_client_count: shadow_snapshots.len(),
            active_client_count,
            changed_instances,
            validation_errors: Vec::new(),
        }
    }
}

fn reload_generation(config: &UdbConfig) -> u64 {
    let mut hasher = Sha256::new();
    if let Ok(bytes) = serde_json::to_vec(config) {
        hasher.update(bytes);
    }
    let digest = hasher.finalize();
    u64::from_be_bytes([
        digest[0], digest[1], digest[2], digest[3], digest[4], digest[5], digest[6], digest[7],
    ])
}

fn shadow_failed_health_checks(runtime: &DataBrokerRuntime) -> Vec<String> {
    runtime
        .backend_instances()
        .iter()
        .filter(|instance| instance.enabled && !instance.connected)
        .map(|instance| format!("{}:{}", instance.backend, instance.name))
        .collect()
}

fn changed_connection_instances(
    previous: &[crate::runtime::connection_manager::ClientSnapshot],
    next: &[crate::runtime::connection_manager::ClientSnapshot],
) -> Vec<String> {
    let previous = previous
        .iter()
        .map(connection_identity)
        .collect::<std::collections::BTreeSet<_>>();
    let next = next
        .iter()
        .map(connection_identity)
        .collect::<std::collections::BTreeSet<_>>();
    previous
        .symmetric_difference(&next)
        .cloned()
        .collect::<Vec<_>>()
}

fn connection_identity(snapshot: &crate::runtime::connection_manager::ClientSnapshot) -> String {
    format!(
        "{}:{}:{}:{}",
        snapshot.project_id, snapshot.backend, snapshot.instance, snapshot.role
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::postgres::PgPoolOptions;

    #[tokio::test]
    async fn reload_dry_run_validates_without_swapping_clients() {
        let mut runtime = DataBrokerRuntime::planning_only();
        let valid_config = UdbConfig {
            primary: DbConfig {
                direct_dsn: "postgres://user:pass@localhost/udb".to_string(),
                ..DbConfig::default()
            },
            ..UdbConfig::default()
        };
        let report = runtime
            .reload_from_config(
                valid_config,
                ConfigReloadOptions {
                    mode: ConfigReloadMode::DryRun,
                    reason: "test".to_string(),
                    ..ConfigReloadOptions::default()
                },
            )
            .await;

        assert!(report.accepted);
        assert!(!report.applied);
        assert!(report.dry_run);
        assert_eq!(report.previous_client_count, 0);
        assert_eq!(runtime.connection_snapshots().len(), 0);
    }

    #[tokio::test]
    async fn reload_shadow_swap_preserves_active_old_lease_as_draining() {
        let old_pool = PgPoolOptions::new()
            .connect_lazy("postgres://user:pass@localhost/old")
            .expect("lazy pool should not connect");
        let new_pool = PgPoolOptions::new()
            .connect_lazy("postgres://user:pass@localhost/new")
            .expect("lazy pool should not connect");
        let mut current = DataBrokerRuntime::planning_only();
        current.connections.register_postgres(
            "primary",
            "read_write",
            old_pool,
            HashMap::from([("generation".to_string(), "old".to_string())]),
        );
        let lease = current
            .connection_manager()
            .lease_postgres("primary")
            .unwrap();

        let mut shadow = DataBrokerRuntime::planning_only();
        shadow.config.default_limit = 250;
        shadow.connections.register_postgres(
            "primary",
            "read_write",
            new_pool,
            HashMap::from([("generation".to_string(), "new".to_string())]),
        );

        let previous = current.connection_snapshots();
        let report = current.apply_shadow_runtime(
            shadow,
            ConfigReloadOptions::default(),
            previous,
            Vec::new(),
        );

        assert!(report.applied);
        assert_eq!(current.config().default_limit, 250);
        let snapshots = current.connection_snapshots();
        assert!(snapshots.iter().any(|snapshot| {
            snapshot.instance == "primary"
                && snapshot.labels.get("generation").map(String::as_str) == Some("new")
        }));
        assert!(snapshots.iter().any(|snapshot| {
            snapshot.state == "draining" && snapshot.instance.starts_with("primary__draining_")
        }));
        drop(lease);
    }

    #[tokio::test]
    async fn reload_rolls_back_when_required_shadow_health_fails() {
        let mut current = DataBrokerRuntime::planning_only();
        let mut shadow = DataBrokerRuntime::planning_only();
        shadow.backend_instances.push(RuntimeBackendInstance {
            name: "analytics".to_string(),
            backend: "clickhouse".to_string(),
            role: "read".to_string(),
            enabled: true,
            configured: true,
            connected: false,
            read_weight: 1,
            write_weight: 0,
            dsn_env: None,
            labels: HashMap::new(),
            capabilities: Vec::new(),
            healthy: false,
            circuit_open: false,
        });

        let report = current.apply_shadow_runtime(
            shadow,
            ConfigReloadOptions {
                require_connected_backends: true,
                rollback_on_failed_health: true,
                ..ConfigReloadOptions::default()
            },
            Vec::new(),
            Vec::new(),
        );

        assert!(!report.applied);
        assert!(report.rolled_back);
        assert_eq!(report.failed_health_checks, vec!["clickhouse:analytics"]);
        assert!(current.backend_instances().is_empty());
    }
}
