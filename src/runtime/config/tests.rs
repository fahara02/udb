use super::*;
use base64::Engine as _;

#[test]
fn default_migration_options() {
    let opts = MigrationOptions::default();
    assert_eq!(opts.migrations_path, "db/migration");
    assert_eq!(opts.seeders_path, "db/seeders");
    assert_eq!(opts.lock_timeout, "5s");
    assert_eq!(opts.statement_timeout, "120s");
    assert_eq!(opts.max_retries, crate::engine::MAX_RETRIES);
    assert!(opts.generate_sql);
    assert!(!opts.emergency_auto_alter);
    assert!(!opts.skip_unchanged_verify);
}

#[test]
fn migration_options_seeders_path_from_env() {
    let prior_seeders = std::env::var("UDB_SEEDERS_PATH").ok();
    #[allow(unused_unsafe)]
    unsafe {
        std::env::set_var("UDB_SEEDERS_PATH", "database/seeders/postgress");
    }

    let opts = MigrationOptions::from_env();
    assert_eq!(opts.seeders_path, "database/seeders/postgress");

    #[allow(unused_unsafe)]
    unsafe {
        match prior_seeders {
            Some(value) => std::env::set_var("UDB_SEEDERS_PATH", value),
            None => std::env::remove_var("UDB_SEEDERS_PATH"),
        }
    }
}

#[test]
fn migration_options_skip_unchanged_verify_is_explicit_opt_out() {
    let prior = std::env::var("UDB_SKIP_UNCHANGED_VERIFY").ok();
    assert!(!MigrationOptions::default().skip_unchanged_verify);

    #[allow(unused_unsafe)]
    unsafe {
        std::env::set_var("UDB_SKIP_UNCHANGED_VERIFY", "true");
    }
    let opts = MigrationOptions::from_env();
    assert!(opts.skip_unchanged_verify);

    restore_env("UDB_SKIP_UNCHANGED_VERIFY", prior);
}

#[test]
fn saga_settings_merge_env_overrides_file_values() {
    let prior_enabled = std::env::var("UDB_SAGA_RECOVERY_ENABLED").ok();
    let prior_interval = std::env::var("UDB_SAGA_RECOVERY_INTERVAL_SECONDS").ok();
    let prior_stale = std::env::var("UDB_SAGA_STALE_THRESHOLD_SECONDS").ok();
    #[allow(unused_unsafe)]
    unsafe {
        std::env::set_var("UDB_SAGA_RECOVERY_ENABLED", "false");
        std::env::set_var("UDB_SAGA_RECOVERY_INTERVAL_SECONDS", "7");
        std::env::set_var("UDB_SAGA_STALE_THRESHOLD_SECONDS", "11");
    }

    let mut config = UdbConfig {
        saga: SagaSettings {
            recovery_enabled: true,
            recovery_interval_secs: 60,
            stale_threshold_secs: 300,
            ..SagaSettings::default()
        },
        ..UdbConfig::default()
    };
    config.merge_env();
    assert!(!config.saga.recovery_enabled);
    assert_eq!(config.saga.recovery_interval_secs, 7);
    assert_eq!(config.saga.stale_threshold_secs, 11);

    restore_env("UDB_SAGA_RECOVERY_ENABLED", prior_enabled);
    restore_env("UDB_SAGA_RECOVERY_INTERVAL_SECONDS", prior_interval);
    restore_env("UDB_SAGA_STALE_THRESHOLD_SECONDS", prior_stale);
}

#[test]
fn circuit_breaker_settings_merge_env_overrides_file_values() {
    let prior_threshold = std::env::var("UDB_CIRCUIT_FAILURE_THRESHOLD").ok();
    let prior_cooldown = std::env::var("UDB_CIRCUIT_COOLDOWN_SECS").ok();
    #[allow(unused_unsafe)]
    unsafe {
        std::env::set_var("UDB_CIRCUIT_FAILURE_THRESHOLD", "9");
        std::env::set_var("UDB_CIRCUIT_COOLDOWN_SECS", "45");
    }

    let mut config = UdbConfig {
        circuit_breaker: CircuitBreakerSettings {
            failure_threshold: 3,
            cooldown_secs: 30,
        },
        ..UdbConfig::default()
    };
    config.merge_env();
    assert_eq!(config.circuit_breaker.failure_threshold, 9);
    assert_eq!(config.circuit_breaker.cooldown_secs, 45);

    restore_env("UDB_CIRCUIT_FAILURE_THRESHOLD", prior_threshold);
    restore_env("UDB_CIRCUIT_COOLDOWN_SECS", prior_cooldown);
}

#[test]
fn project_routing_mode_merge_env_overrides_file_values() {
    let prior = std::env::var("UDB_PROJECT_ROUTING_MODE").ok();
    #[allow(unused_unsafe)]
    unsafe {
        std::env::set_var("UDB_PROJECT_ROUTING_MODE", "strict_with_default:billing");
    }

    let mut config = UdbConfig {
        project_routing_mode: "permissive".to_string(),
        ..UdbConfig::default()
    };
    config.merge_env();
    assert_eq!(config.project_routing_mode, "strict_with_default:billing");

    restore_env("UDB_PROJECT_ROUTING_MODE", prior);
}

#[test]
fn native_services_merge_env_precedence_and_legacy_alias() {
    let prior_new = std::env::var("UDB_NATIVE_SERVICES_ENABLED").ok();
    let prior_old = std::env::var("UDB_NATIVE_AUTH").ok();
    let prior_list = std::env::var("UDB_NATIVE_SERVICES").ok();
    let prior_storage = std::env::var("UDB_NATIVE_STORAGE_ENABLED").ok();
    let prior_storage_migrate = std::env::var("UDB_NATIVE_STORAGE_MIGRATE_ENABLED").ok();

    #[allow(unused_unsafe)]
    unsafe {
        std::env::set_var("UDB_NATIVE_AUTH", "0");
        std::env::set_var("UDB_NATIVE_SERVICES_ENABLED", "1");
        std::env::set_var("UDB_NATIVE_SERVICES", "authn,storage");
        std::env::set_var("UDB_NATIVE_STORAGE_ENABLED", "false");
        std::env::set_var("UDB_NATIVE_STORAGE_MIGRATE_ENABLED", "true");
    }

    let mut cfg = UdbConfig::default();
    cfg.merge_env();
    assert!(
        cfg.native_services.enabled,
        "canonical env wins over legacy alias"
    );
    assert!(!cfg.native_services.default_enabled);
    assert_eq!(cfg.native_services.services["authn"].enabled, Some(true));
    assert_eq!(cfg.native_services.services["storage"].enabled, Some(false));
    assert_eq!(cfg.native_services.services["storage"].migrate, Some(true));

    restore_env("UDB_NATIVE_SERVICES_ENABLED", prior_new);
    restore_env("UDB_NATIVE_AUTH", prior_old);
    restore_env("UDB_NATIVE_SERVICES", prior_list);
    restore_env("UDB_NATIVE_STORAGE_ENABLED", prior_storage);
    restore_env("UDB_NATIVE_STORAGE_MIGRATE_ENABLED", prior_storage_migrate);
}

#[test]
fn production_service_settings_default_to_secure_internal_transport() {
    let prior_env = std::env::var("UDB_ENV").ok();
    let prior_tls = std::env::var("UDB_REQUIRE_SECURE_TRANSPORT").ok();
    let prior_legacy_tls = std::env::var("UDB_TLS_REQUIRED").ok();
    let prior_mtls = std::env::var("UDB_MTLS_REQUIRED").ok();
    let prior_broker_mtls = std::env::var("UDB_BROKER_MTLS_REQUIRED").ok();
    let prior_internal_mtls = std::env::var("UDB_INTERNAL_CONTROL_MTLS_REQUIRED").ok();

    #[allow(unused_unsafe)]
    unsafe {
        std::env::set_var("UDB_ENV", "production");
        std::env::remove_var("UDB_REQUIRE_SECURE_TRANSPORT");
        std::env::remove_var("UDB_TLS_REQUIRED");
        std::env::remove_var("UDB_MTLS_REQUIRED");
        std::env::remove_var("UDB_BROKER_MTLS_REQUIRED");
        std::env::remove_var("UDB_INTERNAL_CONTROL_MTLS_REQUIRED");
    }

    let mut service = ServiceSettings::default();
    service.merge_env();
    assert!(
        service.require_secure_transport,
        "production service listeners must require TLS by default"
    );
    assert!(
        service.mtls_required,
        "production internal/control-plane listeners must require mTLS by default"
    );
    assert!(
        service.broker_to_broker_mtls_required,
        "production broker-to-broker channels must require mTLS by default"
    );
    assert!(
        service.internal_control_mtls_required,
        "production worker/control channels must require mTLS by default"
    );

    restore_env("UDB_ENV", prior_env);
    restore_env("UDB_REQUIRE_SECURE_TRANSPORT", prior_tls);
    restore_env("UDB_TLS_REQUIRED", prior_legacy_tls);
    restore_env("UDB_MTLS_REQUIRED", prior_mtls);
    restore_env("UDB_BROKER_MTLS_REQUIRED", prior_broker_mtls);
    restore_env("UDB_INTERNAL_CONTROL_MTLS_REQUIRED", prior_internal_mtls);
}

#[test]
fn dev_service_settings_do_not_force_secure_transport() {
    let prior_env = std::env::var("UDB_ENV").ok();
    let prior_tls = std::env::var("UDB_REQUIRE_SECURE_TRANSPORT").ok();
    let prior_legacy_tls = std::env::var("UDB_TLS_REQUIRED").ok();
    let prior_mtls = std::env::var("UDB_MTLS_REQUIRED").ok();
    let prior_broker_mtls = std::env::var("UDB_BROKER_MTLS_REQUIRED").ok();
    let prior_internal_mtls = std::env::var("UDB_INTERNAL_CONTROL_MTLS_REQUIRED").ok();

    #[allow(unused_unsafe)]
    unsafe {
        std::env::set_var("UDB_ENV", "development");
        std::env::remove_var("UDB_REQUIRE_SECURE_TRANSPORT");
        std::env::remove_var("UDB_TLS_REQUIRED");
        std::env::remove_var("UDB_MTLS_REQUIRED");
        std::env::remove_var("UDB_BROKER_MTLS_REQUIRED");
        std::env::remove_var("UDB_INTERNAL_CONTROL_MTLS_REQUIRED");
    }

    let mut service = ServiceSettings::default();
    service.merge_env();
    assert!(!service.require_secure_transport);
    assert!(!service.mtls_required);
    assert!(!service.broker_to_broker_mtls_required);
    assert!(!service.internal_control_mtls_required);

    restore_env("UDB_ENV", prior_env);
    restore_env("UDB_REQUIRE_SECURE_TRANSPORT", prior_tls);
    restore_env("UDB_TLS_REQUIRED", prior_legacy_tls);
    restore_env("UDB_MTLS_REQUIRED", prior_mtls);
    restore_env("UDB_BROKER_MTLS_REQUIRED", prior_broker_mtls);
    restore_env("UDB_INTERNAL_CONTROL_MTLS_REQUIRED", prior_internal_mtls);
}

#[test]
fn mtls_umbrella_enables_broker_and_internal_control_mtls() {
    let prior_mtls = std::env::var("UDB_MTLS_REQUIRED").ok();
    let prior_broker_mtls = std::env::var("UDB_BROKER_MTLS_REQUIRED").ok();
    let prior_internal_mtls = std::env::var("UDB_INTERNAL_CONTROL_MTLS_REQUIRED").ok();

    #[allow(unused_unsafe)]
    unsafe {
        std::env::set_var("UDB_MTLS_REQUIRED", "true");
        std::env::remove_var("UDB_BROKER_MTLS_REQUIRED");
        std::env::remove_var("UDB_INTERNAL_CONTROL_MTLS_REQUIRED");
    }

    let mut service = ServiceSettings::default();
    service.merge_env();
    assert!(service.mtls_required);
    assert!(service.broker_to_broker_mtls_required);
    assert!(service.internal_control_mtls_required);

    restore_env("UDB_MTLS_REQUIRED", prior_mtls);
    restore_env("UDB_BROKER_MTLS_REQUIRED", prior_broker_mtls);
    restore_env("UDB_INTERNAL_CONTROL_MTLS_REQUIRED", prior_internal_mtls);
}

#[test]
fn compliance_hardened_yaml_matches_secure_runtime_posture() {
    let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let yaml_path = manifest_dir
        .ancestors()
        .map(|path| path.join("configs/compliance-hardened.yaml"))
        .find(|path| path.is_file())
        .expect(
            "configs/compliance-hardened.yaml should exist in an ancestor of CARGO_MANIFEST_DIR",
        );
    let yaml =
        std::fs::read_to_string(yaml_path).expect("compliance-hardened.yaml should be readable");
    let mut config: UdbConfig =
        serde_yaml::from_str(&yaml).expect("compliance-hardened.yaml must parse as UdbConfig");

    config.service.apply_security_posture(false);

    assert!(
        config.security.validate_production().is_ok(),
        "hardened compliance YAML must satisfy SecurityConfig::validate_production: {:?}",
        config.security.validate_production()
    );
    assert!(
        config.security.is_production(),
        "hardened compliance YAML must put SecurityConfig in production posture"
    );
    assert!(
        config.service.require_secure_transport,
        "hardened compliance YAML must require TLS on service listeners"
    );
    assert!(
        config.service.mtls_required,
        "hardened compliance YAML must require listener mTLS"
    );
    assert!(
        config.service.broker_to_broker_mtls_required,
        "listener mTLS must imply broker-to-broker mTLS"
    );
    assert!(
        config.service.internal_control_mtls_required,
        "listener mTLS must imply worker/control mTLS"
    );
}

#[test]
fn required_object_native_state_encryption_requires_key_source() {
    let mut cfg = UdbConfig::default();
    cfg.primary.direct_dsn = "postgres://user:pass@localhost:5432/udb".to_string();
    cfg.encryption.object_native_state_required = true;

    let report = cfg.validate();
    assert!(
        report
            .errors
            .iter()
            .any(|err| err.contains("object/native-state encryption is required")),
        "required object/native-state encryption must fail closed without key source: {:?}",
        report.errors
    );
}

#[test]
fn required_object_native_state_encryption_accepts_inline_key_source() {
    let mut cfg = UdbConfig::default();
    cfg.primary.direct_dsn = "postgres://user:pass@localhost:5432/udb".to_string();
    cfg.encryption.object_native_state_required = true;
    cfg.encryption.keys.insert(
        1,
        base64::engine::general_purpose::STANDARD.encode([0x42u8; 32]),
    );

    let report = cfg.validate();
    assert!(
        !report
            .errors
            .iter()
            .any(|err| err.contains("object/native-state encryption is required")),
        "configured key source should satisfy object/native-state encryption gate: {:?}",
        report.errors
    );
}

#[test]
fn native_services_yaml_merge_preserves_per_service_overrides() {
    let base = UdbConfig {
        native_services: NativeServicesSettings {
            enabled: true,
            services: [(
                "storage".to_string(),
                NativeServiceConfig {
                    enabled: Some(true),
                    migrate: Some(true),
                    ..NativeServiceConfig::default()
                },
            )]
            .into_iter()
            .collect(),
            ..NativeServicesSettings::default()
        },
        ..UdbConfig::default()
    };
    let file = UdbConfig {
        native_services: NativeServicesSettings {
            default_enabled: false,
            services: [(
                "storage".to_string(),
                NativeServiceConfig {
                    enabled: Some(false),
                    required_backends: vec!["postgres".to_string(), "object_storage".to_string()],
                    ..NativeServiceConfig::default()
                },
            )]
            .into_iter()
            .collect(),
            ..NativeServicesSettings::default()
        },
        ..UdbConfig::default()
    };
    let merged = merge_udb_config(base, file);
    let storage = &merged.native_services.services["storage"];
    assert_eq!(storage.enabled, Some(false));
    assert_eq!(storage.migrate, Some(true));
    assert_eq!(
        storage.required_backends,
        vec!["postgres", "object_storage"]
    );
    assert!(!merged.native_services.default_enabled);
}

#[test]
fn db_config_is_configured() {
    let mut cfg = DbConfig::default();
    assert!(
        !cfg.is_configured(),
        "empty config should not be configured"
    );
    cfg.host = "localhost".to_string();
    cfg.database = "appdb".to_string();
    cfg.role = "app".to_string();
    assert!(cfg.is_configured());
}

#[test]
fn udb_config_has_backup() {
    let mut cfg = UdbConfig::default();
    assert!(!cfg.has_backup());
    cfg.backup = Some(DbConfig {
        host: "backup-host".to_string(),
        database: "appdb".to_string(),
        role: "app".to_string(),
        ..DbConfig::default()
    });
    assert!(cfg.has_backup());
}

#[test]
fn redis_config_effective_port_defaults_to_6379() {
    let cfg = RedisConfig::default();
    assert_eq!(cfg.effective_port(), 6379);
}

#[test]
fn qdrant_config_effective_ports() {
    let cfg = QdrantConfig::default();
    assert_eq!(cfg.effective_grpc_port(), 6334);
    assert_eq!(cfg.effective_http_port(), 6333);
    assert_eq!(cfg.effective_distance(), "Cosine");
}

#[test]
fn minio_config_qualified_bucket() {
    let cfg = MinioConfig {
        endpoint: "http://minio:9000".to_string(),
        bucket_prefix: "tenant-a-".to_string(),
        ..Default::default()
    };
    assert_eq!(cfg.qualified_bucket("artifacts"), "tenant-a-artifacts");
    assert_eq!(cfg.effective_region(), "us-east-1");
}

#[test]
fn udb_config_all_tiers_configured() {
    let cfg = UdbConfig {
        primary: DbConfig {
            host: "pg".to_string(),
            database: "db".to_string(),
            role: "app".to_string(),
            ..Default::default()
        },
        redis: Some(RedisConfig {
            host: "redis".to_string(),
            ..Default::default()
        }),
        qdrant: Some(QdrantConfig {
            host: "qdrant".to_string(),
            ..Default::default()
        }),
        minio: Some(MinioConfig {
            endpoint: "http://minio:9000".to_string(),
            ..Default::default()
        }),
        ..Default::default()
    };
    let tiers = cfg.active_tiers();
    assert_eq!(tiers, vec!["postgres", "redis", "qdrant", "minio"]);
}

#[test]
fn udb_config_postgres_only_active_tier() {
    let cfg = UdbConfig {
        primary: DbConfig {
            host: "pg".to_string(),
            database: "db".to_string(),
            role: "app".to_string(),
            ..Default::default()
        },
        ..Default::default()
    };
    assert_eq!(cfg.active_tiers(), vec!["postgres"]);
    assert!(!cfg.has_redis());
    assert!(!cfg.has_qdrant());
    assert!(!cfg.has_minio());
}

#[test]
fn config_validation_requires_primary() {
    let report = UdbConfig::default().validate();
    assert!(!report.passed);
    assert!(
        report
            .errors
            .iter()
            .any(|error| error.contains("primary PostgreSQL"))
    );
    assert!(
        report
            .health_gates
            .iter()
            .any(|gate| gate.backend == "postgres" && gate.required && !gate.configured)
    );
}

#[test]
fn config_validation_allows_optional_degraded_mode() {
    let cfg = UdbConfig {
        primary: DbConfig {
            host: "pg".to_string(),
            database: "db".to_string(),
            role: "app".to_string(),
            ..Default::default()
        },
        ..Default::default()
    };
    let report = cfg.validate();
    assert!(report.passed);
    assert!(
        report.health_gates.iter().any(|gate| {
            gate.backend == "redis" && !gate.configured && gate.degraded_mode_allowed
        })
    );
}

#[test]
fn backend_instance_config_deserializes_portable_yaml() {
    let yaml = r#"
instances:
  - name: primary
    backend: postgres
    role: read_write
    dsn_env: UDB_PG_DSN_PRIMARY
    labels:
      region: ap-south-1
  - name: vector_a
    backend: qdrant
    role: read
    dsn: http://qdrant:6333
    capabilities:
      - vector_search
"#;
    let config: BackendInstanceConfig = serde_yaml::from_str(yaml).unwrap();
    assert_eq!(config.instances.len(), 2);
    assert_eq!(config.instances[0].routing_key(), "postgres:primary");
    assert_eq!(config.instances[1].routing_key(), "qdrant:vector_a");
    assert!(config.validate().is_empty());
}

#[test]
fn udb_config_loads_toml_backend_instances() {
    let path = std::env::temp_dir().join(format!("udb-config-test-{}.toml", std::process::id()));
    std::fs::write(
        &path,
        r#"
app_name = "udb-test"

[[backend_instances.instances]]
name = "primary"
backend = "postgres"
role = "read_write"
dsn = "postgresql://localhost/udb"

[[backend_instances.instances]]
name = "vector_a"
backend = "qdrant"
role = "read"
dsn = "http://localhost:6333"
"#,
    )
    .unwrap();

    let config = UdbConfig::load_file(&path).unwrap();
    let _ = std::fs::remove_file(&path);

    assert_eq!(config.app_name, "udb-test");
    assert_eq!(config.backend_instances.instances.len(), 2);
    assert_eq!(
        config.backend_instances.instances[1].routing_key(),
        "qdrant:vector_a"
    );
}

#[test]
fn merge_env_preserves_file_backend_instances_without_env_override() {
    let key = "UDB_BACKEND_INSTANCES";
    let prior = std::env::var(key).ok();
    #[allow(unused_unsafe)]
    unsafe {
        std::env::remove_var(key);
    }

    let mut config = UdbConfig {
        backend_instances: BackendInstanceConfig {
            instances: vec![BackendInstance {
                name: "analytics".to_string(),
                backend: "clickhouse".to_string(),
                role: BackendInstanceRole::Read,
                dsn: Some("http://clickhouse:8123".to_string()),
                dsn_env: None,
                ..BackendInstance::default()
            }],
        },
        ..UdbConfig::default()
    };
    config.merge_env();
    assert_eq!(config.backend_instances.instances.len(), 1);
    assert_eq!(
        config.backend_instances.instances[0].routing_key(),
        "clickhouse:analytics"
    );

    #[allow(unused_unsafe)]
    unsafe {
        match prior {
            Some(value) => std::env::set_var(key, value),
            None => std::env::remove_var(key),
        }
    }
}

#[test]
fn udb_config_examples_load_and_validate() {
    let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("configs");
    let mut checked = 0usize;
    for entry in std::fs::read_dir(&root).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("yaml") {
            continue;
        }
        let config = UdbConfig::load_file(&path)
            .unwrap_or_else(|err| panic!("{} failed to load: {err}", path.display()));
        let report = config.validate();
        assert!(
            report.passed,
            "{} failed validation: {:?}",
            path.display(),
            report.errors
        );
        checked += 1;
    }
    assert!(
        checked >= 4,
        "expected runtime YAML examples in udb/configs"
    );
}

#[test]
fn deterministic_config_merge_preserves_precedence() {
    let base = UdbConfig {
        app_name: "default".to_string(),
        default_limit: 100,
        max_limit: 1000,
        primary: DbConfig {
            direct_dsn: "postgresql://base/udb".to_string(),
            ..DbConfig::default()
        },
        ..UdbConfig::default()
    };
    let file = UdbConfig {
        app_name: "file".to_string(),
        default_limit: 250,
        primary: DbConfig {
            direct_dsn: "postgresql://file/udb".to_string(),
            ..DbConfig::default()
        },
        backend_instances: BackendInstanceConfig {
            instances: vec![BackendInstance {
                name: "file_primary".to_string(),
                backend: "postgres".to_string(),
                dsn: Some("postgresql://file/udb".to_string()),
                dsn_env: None,
                ..BackendInstance::default()
            }],
        },
        ..UdbConfig::default()
    };
    let merged = merge_udb_config(base, file);
    assert_eq!(merged.app_name, "file");
    assert_eq!(merged.default_limit, 250);
    assert_eq!(merged.max_limit, 1000);
    assert_eq!(merged.primary.direct_dsn, "postgresql://file/udb");
    assert_eq!(merged.backend_instances.instances[0].name, "file_primary");
}

#[test]
fn config_validation_rejects_bad_instance_dsn_and_capabilities() {
    let config = BackendInstanceConfig {
        instances: vec![
            BackendInstance {
                name: "bad_dsn".to_string(),
                backend: "redis".to_string(),
                dsn: Some("not a dsn".to_string()),
                dsn_env: None,
                ..BackendInstance::default()
            },
            BackendInstance {
                name: "analytics".to_string(),
                backend: "clickhouse".to_string(),
                dsn: Some("http://clickhouse:8123".to_string()),
                dsn_env: None,
                capabilities: ["transaction_required".to_string()].into_iter().collect(),
                ..BackendInstance::default()
            },
        ],
    };
    let errors = config.validate();
    assert!(
        errors
            .iter()
            .any(|error| error.contains("does not look valid"))
    );
    assert!(
        errors
            .iter()
            .any(|error| error.contains("unsupported transactional writes"))
    );
}

#[test]
fn backend_instance_config_rejects_duplicate_and_unknown_backend() {
    let config = BackendInstanceConfig {
        instances: vec![
            BackendInstance {
                name: "primary".to_string(),
                backend: "postgres".to_string(),
                dsn_env: Some("UDB_PG_DSN".to_string()),
                ..BackendInstance::default()
            },
            BackendInstance {
                name: "primary".to_string(),
                backend: "postgres".to_string(),
                dsn_env: Some("UDB_PG_DSN_OTHER".to_string()),
                ..BackendInstance::default()
            },
            BackendInstance {
                name: "default".to_string(),
                backend: "notadb".to_string(),
                dsn_env: Some("UDB_NOTADB_DSN".to_string()),
                ..BackendInstance::default()
            },
        ],
    };
    let errors = config.validate();
    assert!(
        errors
            .iter()
            .any(|error| error.contains("duplicate backend instance 'postgres:primary'"))
    );
    assert!(
        errors
            .iter()
            .any(|error| error.contains("unsupported backend 'notadb'"))
    );
}

#[cfg(not(feature = "mongodb-native"))]
#[test]
fn mongodb_instance_requires_data_api_endpoint() {
    let config = BackendInstanceConfig {
        instances: vec![BackendInstance {
            name: "document".to_string(),
            backend: "mongodb".to_string(),
            dsn: Some("mongodb://localhost:27017/udb".to_string()),
            dsn_env: None,
            ..BackendInstance::default()
        }],
    };
    let errors = config.validate();
    assert!(errors.iter().any(|error| error.contains("Data API")));
    assert!(
        errors
            .iter()
            .any(|error| { error.contains("native MongoDB DSN without a Data API URL") })
    );
}

#[test]
fn mongodb_data_api_instance_validates() {
    let config = BackendInstanceConfig {
        instances: vec![BackendInstance {
            name: "document".to_string(),
            backend: "mongodb".to_string(),
            dsn: Some("mongodb://localhost:27017/udb".to_string()),
            dsn_env: None,
            labels: [(
                "api_url".to_string(),
                "https://data.mongodb-api.com/app/app/endpoint/data/v1".to_string(),
            )]
            .into_iter()
            .collect(),
            ..BackendInstance::default()
        }],
    };
    assert!(config.validate().is_empty());
}

#[cfg(not(feature = "mongodb-native"))]
#[test]
fn mongodb_srv_instance_is_rejected_without_native_driver() {
    let config = BackendInstanceConfig {
        instances: vec![BackendInstance {
            name: "document".to_string(),
            backend: "mongodb".to_string(),
            dsn: Some("mongodb+srv://cluster.example.com/udb".to_string()),
            dsn_env: None,
            labels: [(
                "api_url".to_string(),
                "https://data.mongodb-api.com/app/app/endpoint/data/v1".to_string(),
            )]
            .into_iter()
            .collect(),
            ..BackendInstance::default()
        }],
    };
    assert!(
        config
            .validate()
            .iter()
            .any(|error| error.contains("mongodb+srv://"))
    );
}

#[cfg(feature = "mongodb-native")]
#[test]
fn mongodb_native_instance_validates_with_native_driver() {
    let config = BackendInstanceConfig {
        instances: vec![BackendInstance {
            name: "document".to_string(),
            backend: "mongodb".to_string(),
            dsn: Some("mongodb://localhost:27017/udb".to_string()),
            dsn_env: None,
            labels: [("transport".to_string(), "native".to_string())]
                .into_iter()
                .collect(),
            ..BackendInstance::default()
        }],
    };
    assert!(config.validate().is_empty());
}

#[test]
fn udb_config_active_tiers_include_backend_instances() {
    let cfg = UdbConfig {
        primary: DbConfig {
            host: "pg".to_string(),
            database: "db".to_string(),
            role: "app".to_string(),
            ..Default::default()
        },
        backend_instances: BackendInstanceConfig {
            instances: vec![BackendInstance {
                name: "analytics".to_string(),
                backend: "clickhouse".to_string(),
                role: BackendInstanceRole::Read,
                dsn: Some("http://clickhouse:8123".to_string()),
                dsn_env: None,
                ..BackendInstance::default()
            }],
        },
        ..Default::default()
    };
    assert!(cfg.active_tiers().contains(&"clickhouse"));
    assert!(cfg.validate().passed);
}

#[test]
fn audit_sink_config_defaults_to_none() {
    let cfg = AuditSinkConfig::default();
    assert!(!cfg.is_active());
}

#[test]
fn audit_sink_file_without_path_fails_validation() {
    let cfg = AuditSinkConfig {
        kind: AuditSinkKind::File,
        min_severity: "info".to_string(),
        ..Default::default()
    };
    let errors = cfg.validate();
    assert!(!errors.is_empty());
}

#[test]
fn audit_sink_kafka_with_topic_and_brokers_passes() {
    let cfg = AuditSinkConfig {
        kind: AuditSinkKind::Kafka,
        kafka_topic: Some("udb.audit".to_string()),
        kafka_brokers: Some("localhost:9092".to_string()),
        min_severity: "info".to_string(),
        ..Default::default()
    };
    assert!(cfg.validate().is_empty());
}

#[test]
fn audit_sink_invalid_min_severity_fails() {
    let cfg = AuditSinkConfig {
        kind: AuditSinkKind::Stdout,
        min_severity: "debug".to_string(),
        ..Default::default()
    };
    let errors = cfg.validate();
    assert!(errors.iter().any(|e| e.contains("debug")));
}

fn restore_env(key: &str, value: Option<String>) {
    #[allow(unused_unsafe)]
    unsafe {
        match value {
            Some(value) => std::env::set_var(key, value),
            None => std::env::remove_var(key),
        }
    }
}

#[test]
fn pg_dsn_from_libpq_env_assembles_neon_style_dsn() {
    let keys = [
        "PGHOST",
        "PGPORT",
        "PGUSER",
        "PGPASSWORD",
        "PGDATABASE",
        "PGSSLMODE",
    ];
    let saved: Vec<_> = keys.iter().map(|k| (*k, std::env::var(k).ok())).collect();

    restore_env("PGHOST", Some("ep-demo.neon.tech".to_string()));
    restore_env("PGUSER", Some("neondb_owner".to_string()));
    // Password with a reserved char must be percent-encoded.
    restore_env("PGPASSWORD", Some("p@ss/word".to_string()));
    restore_env("PGDATABASE", Some("neondb".to_string()));
    restore_env("PGSSLMODE", Some("require".to_string()));
    restore_env("PGPORT", None);

    assert_eq!(
        super::pg_dsn_from_libpq_env().as_deref(),
        Some(
            "postgresql://neondb_owner:p%40ss%2Fword@ep-demo.neon.tech:5432/neondb?sslmode=require"
        )
    );

    // Without the minimum required parts, no DSN is fabricated.
    restore_env("PGHOST", None);
    assert!(super::pg_dsn_from_libpq_env().is_none());

    for (key, value) in saved {
        restore_env(key, value);
    }
}
