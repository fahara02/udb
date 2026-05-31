//! Free helper functions split out of core.rs (Phase F).
use super::*;
#[cfg(any(
    feature = "qdrant",
    feature = "s3",
    feature = "mongodb",
    feature = "neo4j",
    feature = "clickhouse"
))]
use std::env;

pub(crate) fn runtime_backend_instances(
    config: &BackendInstanceConfig,
    report: &RuntimeInitReport,
    runtime: &DataBrokerRuntime,
) -> Vec<RuntimeBackendInstance> {
    config
        .instances
        .iter()
        .map(|instance| runtime_backend_instance(instance, report, runtime))
        .collect()
}

pub(crate) fn runtime_backend_instance(
    instance: &BackendInstance,
    report: &RuntimeInitReport,
    runtime: &DataBrokerRuntime,
) -> RuntimeBackendInstance {
    let backend = instance
        .canonical_backend()
        .map(|kind| kind.as_str().to_string())
        .unwrap_or_else(|| instance.backend.to_ascii_lowercase());
    let connected = match backend.as_str() {
        "postgres" => {
            runtime.pg_instances.contains_key(&instance.name)
                || (instance.name == "primary" && report.postgres_configured)
        }
        #[cfg(feature = "redis")]
        "redis" => {
            runtime.redis_instances.contains_key(&instance.name)
                || (instance.name == "default" && report.redis_configured)
        }
        #[cfg(feature = "qdrant")]
        "qdrant" => {
            runtime.qdrant_instances.contains_key(&instance.name)
                || (instance.name == "default" && report.qdrant_configured)
        }
        #[cfg(feature = "s3")]
        "minio" | "s3" => {
            runtime.s3_instances.contains_key(&instance.name)
                || (instance.name == "default" && report.s3_configured)
        }
        #[cfg(feature = "mongodb")]
        "mongodb" => {
            runtime.mongodb_instances.contains_key(&instance.name)
                || (instance.name == "default" && report.mongodb_configured)
        }
        #[cfg(feature = "neo4j")]
        "neo4j" => {
            runtime.neo4j_instances.contains_key(&instance.name)
                || (instance.name == "default" && report.neo4j_configured)
        }
        #[cfg(feature = "clickhouse")]
        "clickhouse" => {
            runtime.clickhouse_instances.contains_key(&instance.name)
                || (instance.name == "default" && report.clickhouse_configured)
        }
        _ => backend_connected(report, &backend),
    };
    let circuit_open = !runtime.circuit_breaker_allows(&backend, Some(&instance.name));
    RuntimeBackendInstance {
        name: instance.name.clone(),
        backend,
        role: instance.role.as_str().to_string(),
        enabled: instance.enabled,
        configured: instance_has_config_reference(instance) || connected,
        connected,
        read_weight: instance.read_weight,
        write_weight: instance.write_weight,
        dsn_env: instance.dsn_env.clone(),
        labels: instance
            .labels
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
        capabilities: instance.capabilities.iter().cloned().collect(),
        healthy: instance.enabled && connected && !circuit_open,
        circuit_open,
    }
}

fn instance_has_config_reference(instance: &BackendInstance) -> bool {
    instance
        .dsn
        .as_ref()
        .map(|value| !value.trim().is_empty())
        .unwrap_or(false)
        || instance
            .dsn_env
            .as_ref()
            .map(|value| !value.trim().is_empty())
            .unwrap_or(false)
}

pub(crate) fn backend_connected(report: &RuntimeInitReport, backend: &str) -> bool {
    match backend {
        "postgres" => report.postgres_configured,
        "redis" => report.redis_configured,
        "qdrant" => report.qdrant_configured,
        "minio" | "s3" => report.s3_configured,
        "mongodb" => report.mongodb_configured,
        "neo4j" => report.neo4j_configured,
        "clickhouse" => report.clickhouse_configured,
        _ => false,
    }
}

pub(crate) fn circuit_key(backend: &str, instance: Option<&str>) -> String {
    format!(
        "{}:{}",
        backend.to_ascii_lowercase(),
        instance
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or("default")
            .to_ascii_lowercase()
    )
}

pub(crate) fn unix_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or_default()
}

pub(crate) fn build_executor_registry(
    instances: &[RuntimeBackendInstance],
) -> BackendExecutorRegistry {
    let mut registry = BackendExecutorRegistry::default();
    for instance in instances {
        if !instance.enabled {
            continue;
        }
        registry.register(BackendExecutorRegistration {
            backend: instance.backend.clone(),
            instance: Some(instance.name.clone()),
            role: instance.role.clone(),
            connected: instance.connected,
        });
    }
    registry
}

pub(crate) fn split_backend_selector(selector: &str) -> (&str, Option<&str>) {
    selector
        .split_once(':')
        .or_else(|| selector.split_once('.'))
        .map(|(backend, instance)| {
            let instance = instance.trim();
            (
                backend.trim(),
                if instance.is_empty() {
                    None
                } else {
                    Some(instance)
                },
            )
        })
        .unwrap_or((selector.trim(), None))
}

pub(crate) fn parse_dispatch_json(request_json: &str) -> Result<JsonValue, tonic::Status> {
    if request_json.trim().is_empty() {
        return Ok(serde_json::json!({}));
    }
    serde_json::from_str(request_json)
        .map_err(|err| tonic::Status::invalid_argument(format!("invalid spec_json: {err}")))
}

// `json_required_str`, `json_required_f32_vec`, `json_i32`, and `json_bool` now
// live in `executor_utils` (imported via `use crate::runtime::executor_utils::*`) so the
// per-backend executors can share them.

// `object_bytes_from_json` now lives in `executor_utils` (imported via the glob).

pub(crate) fn dispatch_params(value: &JsonValue) -> Result<Vec<JsonValue>, tonic::Status> {
    match value.get("params").or_else(|| value.get("parameters")) {
        Some(JsonValue::Array(params)) => Ok(params.clone()),
        Some(_) => Err(tonic::Status::invalid_argument(
            "params/parameters must be an array",
        )),
        None => Ok(Vec::new()),
    }
}

pub(crate) fn validate_single_statement(sql: &str) -> Result<(), tonic::Status> {
    let trimmed = sql.trim();
    if trimmed.is_empty() {
        return Err(tonic::Status::invalid_argument("sql is required"));
    }
    let semicolon_count = trimmed.chars().filter(|ch| *ch == ';').count();
    if semicolon_count > 1 || (semicolon_count == 1 && !trimmed.ends_with(';')) {
        return Err(tonic::Status::invalid_argument(
            "generic PostgreSQL dispatch accepts exactly one statement",
        ));
    }
    Ok(())
}

pub(crate) fn validate_pg_read_sql(sql: &str) -> Result<(), tonic::Status> {
    validate_single_statement(sql)?;
    let first = sql
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .trim_end_matches(';')
        .to_ascii_lowercase();
    if matches!(first.as_str(), "select" | "with" | "show" | "explain") {
        Ok(())
    } else {
        Err(tonic::Status::failed_precondition(
            "generic PostgreSQL query allows only SELECT, WITH, SHOW, or EXPLAIN",
        ))
    }
}

pub(crate) fn validate_pg_mutation_sql(sql: &str) -> Result<(), tonic::Status> {
    validate_single_statement(sql)?;
    let first = sql
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .trim_end_matches(';')
        .to_ascii_lowercase();
    if matches!(first.as_str(), "insert" | "update" | "delete") {
        Ok(())
    } else {
        Err(tonic::Status::failed_precondition(
            "generic PostgreSQL mutate allows only INSERT, UPDATE, or DELETE",
        ))
    }
}

pub(crate) fn bind_generic_pg_params<'q>(
    mut query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    params: &'q [JsonValue],
) -> sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
    for value in params {
        query = match value {
            JsonValue::Null => query.bind(Option::<String>::None),
            JsonValue::Bool(value) => query.bind(*value),
            JsonValue::Number(number) => {
                if let Some(value) = number.as_i64() {
                    query.bind(value)
                } else if let Some(value) = number.as_u64() {
                    if let Ok(value) = i64::try_from(value) {
                        query.bind(value)
                    } else {
                        query.bind(value.to_string())
                    }
                } else {
                    query.bind(number.as_f64().unwrap_or_default())
                }
            }
            JsonValue::String(value) => query.bind(value.clone()),
            JsonValue::Array(_) | JsonValue::Object(_) => {
                query.bind(sqlx::types::Json(value.clone()))
            }
        };
    }
    query
}

pub(crate) fn pg_rows_to_json(rows: Vec<PgRow>) -> Result<Vec<JsonValue>, tonic::Status> {
    rows.into_iter()
        .map(|row| {
            let mut object = serde_json::Map::new();
            for (idx, column) in row.columns().iter().enumerate() {
                object.insert(
                    column.name().to_string(),
                    row_value_to_json(&row, idx, column.type_info().name())?,
                );
            }
            Ok(JsonValue::Object(object))
        })
        .collect()
}

pub(crate) async fn connect_pg_pool_from_config(
    dsn: &str,
    app_name: &str,
    config: &DbConfig,
) -> Result<PgPool, String> {
    let acquire_timeout = Duration::from_secs(if config.acquire_timeout_secs > 0 {
        config.acquire_timeout_secs
    } else {
        10
    });
    let idle_timeout = Duration::from_secs(if config.conn_max_idle_secs > 0 {
        config.conn_max_idle_secs
    } else {
        600
    });
    let max_lifetime = Duration::from_secs(if config.conn_max_lifetime_secs > 0 {
        config.conn_max_lifetime_secs
    } else {
        1800
    });
    let connection_string = append_application_name(dsn, app_name);
    let min_connections = if config.min_connections > 0 {
        config.min_connections as u32
    } else {
        5
    };
    let max_connections = if config.max_open_conns > 0 {
        config.max_open_conns as u32
    } else {
        50
    };
    PgPoolOptions::new()
        .min_connections(min_connections)
        .max_connections(max_connections)
        .acquire_timeout(acquire_timeout)
        .idle_timeout(idle_timeout)
        .max_lifetime(max_lifetime)
        .test_before_acquire(true)
        .after_connect(|conn, _| {
            Box::pin(async move {
                // GAP-KEEP: Enable server-side TCP keepalive probes to prevent
                // OS/NAT devices from dropping the connection during long seed
                // batches (Windows os error 10053 / WSAECONNABORTED).
                // Ignore errors — some hosted providers (Neon) may not expose
                // these GUCs under serverless / connection-pooler mode.
                let _ = conn.execute("SET tcp_keepalives_idle = 60").await;
                let _ = conn.execute("SET tcp_keepalives_interval = 10").await;
                let _ = conn.execute("SET tcp_keepalives_count = 5").await;
                conn.execute("SET statement_timeout = 30000").await?;
                Ok(())
            })
        })
        .connect(&connection_string)
        .await
        .map_err(|err| err.to_string())
}

pub(crate) fn postgres_dsn_from_config(config: &DbConfig) -> Option<String> {
    if !config.direct_dsn.trim().is_empty() {
        return Some(config.direct_dsn.trim().to_string());
    }
    if !config.pooler_dsn.trim().is_empty() {
        return Some(config.pooler_dsn.trim().to_string());
    }
    if let Some(dsn) = config
        .deploy
        .dsn
        .as_ref()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
    {
        return Some(dsn);
    }
    if !config.is_configured() {
        return None;
    }
    let port = if config.port > 0 { config.port } else { 5432 };
    let user = urlencoding::encode(&config.role);
    let password = urlencoding::encode(&config.password);
    let auth = if config.password.is_empty() {
        user.to_string()
    } else {
        format!("{user}:{password}")
    };
    let database = urlencoding::encode(&config.database);
    Some(format!(
        "postgresql://{auth}@{}:{port}/{database}?sslmode={}",
        config.host,
        config.effective_ssl_mode()
    ))
}

#[cfg(feature = "redis")]
pub(crate) fn redis_dsn_from_config(config: &RedisConfig) -> Option<String> {
    if let Some(dsn) = config
        .dsn
        .as_ref()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
    {
        return Some(dsn);
    }
    if !config.is_configured() {
        return None;
    }
    let scheme = if config.effective_tls() {
        "rediss"
    } else {
        "redis"
    };
    let auth = if config.password.is_empty() {
        String::new()
    } else {
        format!(":{}@", urlencoding::encode(&config.password))
    };
    Some(format!(
        "{scheme}://{auth}{}:{}/{}",
        config.host,
        config.effective_port(),
        config.database
    ))
}

#[cfg(feature = "qdrant")]
pub(crate) fn qdrant_url_from_config(
    config: &crate::runtime::config::QdrantConfig,
) -> Option<String> {
    if let Some(url) = config
        .url
        .as_ref()
        .map(|value| value.trim().trim_end_matches('/').to_string())
        .filter(|value| !value.is_empty())
    {
        return Some(url);
    }
    if !config.is_configured() {
        return None;
    }
    let scheme = if config.effective_tls() {
        "https"
    } else {
        "http"
    };
    Some(format!(
        "{scheme}://{}:{}",
        config.host,
        config.effective_http_port()
    ))
}

#[cfg(feature = "s3")]
pub(crate) async fn s3_client_from_config(
    config: &MinioConfig,
) -> Result<aws_sdk_s3::Client, String> {
    if !config.is_configured() {
        return Err("endpoint is empty".to_string());
    }
    if config.access_key.trim().is_empty() || config.secret_key.trim().is_empty() {
        return Err("access key or secret key is missing".to_string());
    }
    let sdk_config = aws_config::defaults(BehaviorVersion::latest())
        .endpoint_url(config.endpoint.clone())
        .region(Region::new(config.effective_region().to_string()))
        .credentials_provider(Credentials::new(
            config.access_key.clone(),
            config.secret_key.clone(),
            None,
            None,
            "udb-config",
        ))
        .load()
        .await;
    let s3_config = aws_sdk_s3::config::Builder::from(&sdk_config)
        .force_path_style(true)
        .build();
    Ok(aws_sdk_s3::Client::from_conf(s3_config))
}

#[cfg(any(
    feature = "qdrant",
    feature = "s3",
    feature = "mongodb",
    feature = "neo4j",
    feature = "clickhouse"
))]
pub(crate) fn instance_label<'a>(instance: &'a BackendInstance, keys: &[&str]) -> Option<&'a str> {
    keys.iter()
        .find_map(|key| instance.labels.get(*key).map(String::as_str))
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

#[cfg(any(
    feature = "qdrant",
    feature = "s3",
    feature = "mongodb",
    feature = "neo4j",
    feature = "clickhouse"
))]
pub(crate) fn instance_label_or_env(instance: &BackendInstance, keys: &[&str]) -> Option<String> {
    if let Some(value) = instance_label(instance, keys) {
        return Some(value.to_string());
    }
    let env_keys = keys
        .iter()
        .filter_map(|key| instance.labels.get(format!("{key}_env").as_str()));
    for key in env_keys {
        if let Ok(value) = env::var(key)
            && !value.trim().is_empty()
        {
            return Some(value);
        }
    }
    None
}

#[cfg(any(feature = "mongodb", feature = "neo4j", feature = "clickhouse"))]
pub(crate) fn instance_label_bool(instance: &BackendInstance, keys: &[&str]) -> Option<bool> {
    instance_label(instance, keys).map(|value| {
        !matches!(
            value.to_ascii_lowercase().as_str(),
            "0" | "false" | "no" | "off"
        )
    })
}

#[cfg(any(feature = "mongodb", feature = "neo4j", feature = "clickhouse"))]
pub(crate) fn instance_label_u64(instance: &BackendInstance, keys: &[&str]) -> Option<u64> {
    instance_label(instance, keys).and_then(|value| value.parse::<u64>().ok())
}

#[cfg(any(feature = "mongodb", feature = "neo4j", feature = "clickhouse"))]
pub(crate) fn instance_is_cloud(
    instance: &BackendInstance,
    base_url: &str,
    cloud_marker: &str,
) -> bool {
    if let Some(value) = instance_label(instance, &["deploy_mode", "mode", "deployment"]) {
        return matches!(
            value.to_ascii_lowercase().replace('-', "_").as_str(),
            "cloud" | "managed" | "hosted"
        );
    }
    !cloud_marker.is_empty() && base_url.contains(cloud_marker)
}

#[cfg(feature = "qdrant")]
pub(crate) fn qdrant_client_from_instance(instance: &BackendInstance) -> Option<QdrantHttpClient> {
    let url = instance.resolve_dsn()?;
    let timeout_secs = instance
        .labels
        .get("timeout_secs")
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(30);
    let http = crate::runtime::executors::http::HttpClientSpec::with_timeout(Duration::from_secs(
        timeout_secs,
    ))
    .build();
    Some(QdrantHttpClient {
        base_url: url.trim_end_matches('/').to_string(),
        api_key: instance_label_or_env(instance, &["api_key"]),
        http,
    })
}

#[cfg(feature = "s3")]
pub(crate) async fn s3_client_from_instance(
    instance: &BackendInstance,
) -> Result<Option<aws_sdk_s3::Client>, String> {
    let Some(endpoint) = instance.resolve_dsn() else {
        return Ok(None);
    };
    let Some(access_key) = instance_label_or_env(instance, &["access_key", "aws_access_key_id"])
    else {
        return Err("access key is missing".to_string());
    };
    let Some(secret_key) =
        instance_label_or_env(instance, &["secret_key", "aws_secret_access_key"])
    else {
        return Err("secret key is missing".to_string());
    };
    let region = instance_label(instance, &["region", "aws_region"])
        .unwrap_or("us-east-1")
        .to_string();
    let sdk_config = aws_config::defaults(BehaviorVersion::latest())
        .endpoint_url(endpoint)
        .region(Region::new(region))
        .credentials_provider(Credentials::new(
            access_key,
            secret_key,
            None,
            None,
            "udb-instance",
        ))
        .load()
        .await;
    let s3_config = aws_sdk_s3::config::Builder::from(&sdk_config)
        .force_path_style(true)
        .build();
    Ok(Some(aws_sdk_s3::Client::from_conf(s3_config)))
}

#[cfg(feature = "mongodb")]
pub(crate) async fn mongodb_executor_from_instance(
    instance: &BackendInstance,
) -> Result<Option<MongoDbExecutor>, String> {
    let dsn = instance.resolve_dsn();
    #[cfg(feature = "mongodb-native")]
    {
        let transport = instance_label(instance, &["transport"])
            .map(|value| value.trim().to_ascii_lowercase())
            .unwrap_or_default();
        let dsn_is_native = dsn.as_ref().is_some_and(|value| {
            let value = value.trim().to_ascii_lowercase();
            value.starts_with("mongodb://") || value.starts_with("mongodb+srv://")
        });
        let has_api_label = instance_label(instance, &["api_base", "api_url"]).is_some()
            || instance_label_or_env(instance, &["api_url", "api_base"]).is_some();
        if transport == "native" || (dsn_is_native && !has_api_label) {
            let dsn = dsn.ok_or_else(|| {
                format!(
                    "MongoDB instance '{}' requests native transport but no dsn/dsn_env is set",
                    instance.name
                )
            })?;
            let database = instance_label(instance, &["database", "db"])
                .map(ToString::to_string)
                .or_else(|| MongoDbConfig::db_from_dsn(&dsn))
                .unwrap_or_else(|| "udb".to_string());
            let timeout_secs =
                instance_label_u64(instance, &["timeout_secs", "request_timeout_secs"])
                    .unwrap_or(30);
            let max_pool_size = instance_label_u64(instance, &["max_pool_size", "pool_size"])
                .and_then(|value| u32::try_from(value).ok());
            return MongoDbExecutor::new_native(MongoDbNativeConfig {
                dsn,
                database,
                timeout_secs,
                app_name: instance_label(instance, &["app_name"]).map(ToString::to_string),
                max_pool_size,
                direct_connection: instance_label_bool(instance, &["direct_connection"]),
                retry_writes: instance_label_bool(instance, &["retry_writes"]),
            })
            .await
            .map(Some);
        }
    }

    let api_base = instance_label(instance, &["api_base", "api_url"])
        .map(ToString::to_string)
        .or_else(|| instance_label_or_env(instance, &["api_url", "api_base"]))
        .ok_or_else(|| {
            format!(
                "MongoDB instance '{}' requires labels.api_url/api_base or labels.api_url_env/api_base_env; native mongodb:// DSNs are not supported",
                instance.name
            )
        })?;
    let database = instance_label(instance, &["database", "db"])
        .map(ToString::to_string)
        .or_else(|| dsn.as_deref().and_then(MongoDbConfig::db_from_dsn))
        .unwrap_or_else(|| "udb".to_string());
    Ok(Some(MongoDbExecutor::new(MongoDbConfig {
        is_cloud: instance_is_cloud(instance, &api_base, ".mongodb.net"),
        dev_mode: instance_label_bool(instance, &["dev_mode"]).unwrap_or(false),
        timeout_secs: instance_label_u64(instance, &["timeout_secs", "request_timeout_secs"])
            .unwrap_or(30),
        api_base,
        api_key: instance_label_or_env(instance, &["api_key"]),
        database,
    })))
}

#[cfg(feature = "neo4j")]
pub(crate) fn neo4j_executor_from_instance(instance: &BackendInstance) -> Option<Neo4jExecutor> {
    let dsn = instance.resolve_dsn();
    let http_base = instance_label(instance, &["http_base", "http_url"])
        .map(ToString::to_string)
        .or_else(|| dsn.as_deref().map(Neo4jConfig::http_base_from_dsn))?;
    Some(Neo4jExecutor::new(Neo4jConfig {
        is_cloud: instance_is_cloud(instance, &http_base, ".databases.neo4j.io"),
        dev_mode: instance_label_bool(instance, &["dev_mode"]).unwrap_or(false),
        timeout_secs: instance_label_u64(instance, &["timeout_secs", "request_timeout_secs"])
            .unwrap_or(30),
        http_base,
        username: instance_label(instance, &["username", "user"])
            .unwrap_or("neo4j")
            .to_string(),
        password: instance_label_or_env(instance, &["password"]).unwrap_or_default(),
        database: instance_label(instance, &["database", "db"])
            .unwrap_or("neo4j")
            .to_string(),
    }))
}

#[cfg(feature = "clickhouse")]
pub(crate) fn clickhouse_executor_from_instance(
    instance: &BackendInstance,
) -> Option<ClickHouseExecutor> {
    let dsn = instance.resolve_dsn();
    let http_base = instance_label(instance, &["http_base", "http_url"])
        .map(ToString::to_string)
        .or_else(|| dsn.as_deref().map(ClickHouseConfig::http_base_from_dsn))?;
    let database = instance_label(instance, &["database", "db"])
        .map(ToString::to_string)
        .or_else(|| dsn.as_deref().and_then(ClickHouseConfig::db_from_dsn))
        .unwrap_or_else(|| "default".to_string());
    Some(ClickHouseExecutor::new(ClickHouseConfig {
        is_cloud: instance_is_cloud(instance, &http_base, ".clickhouse.cloud"),
        connect_timeout_secs: instance_label_u64(
            instance,
            &["connect_timeout_secs", "timeout_secs"],
        )
        .unwrap_or(10),
        query_timeout_secs: instance_label_u64(instance, &["query_timeout_secs"]).unwrap_or(30),
        http_base,
        username: instance_label(instance, &["username", "user"])
            .unwrap_or("default")
            .to_string(),
        password: instance_label_or_env(instance, &["password"]).unwrap_or_default(),
        database,
    }))
}

pub(crate) fn effective_app_name(config: &UdbConfig) -> String {
    if config.app_name.trim().is_empty() {
        "udb".to_string()
    } else {
        config.app_name.trim().to_string()
    }
}

pub(crate) fn effective_backend_instance_config(config: &UdbConfig) -> BackendInstanceConfig {
    if !config.backend_instances.instances.is_empty() {
        return config.backend_instances.clone();
    }

    let mut instances = Vec::new();
    if let Some(dsn) = postgres_dsn_from_config(&config.primary) {
        instances.push(BackendInstance {
            backend: "postgres".to_string(),
            name: "primary".to_string(),
            role: BackendInstanceRole::ReadWrite,
            dsn: Some(dsn),
            dsn_env: None,
            ..BackendInstance::default()
        });
    }
    #[cfg(feature = "redis")]
    if let Some(redis) = &config.redis
        && let Some(dsn) = redis_dsn_from_config(redis)
    {
        instances.push(BackendInstance {
            backend: "redis".to_string(),
            name: "default".to_string(),
            role: BackendInstanceRole::ReadWrite,
            dsn: Some(dsn),
            dsn_env: None,
            ..BackendInstance::default()
        });
    }
    #[cfg(feature = "qdrant")]
    if let Some(qdrant) = &config.qdrant
        && let Some(url) = qdrant_url_from_config(qdrant)
    {
        instances.push(BackendInstance {
            backend: "qdrant".to_string(),
            name: "default".to_string(),
            role: BackendInstanceRole::ReadWrite,
            dsn: Some(url),
            dsn_env: None,
            ..BackendInstance::default()
        });
    }
    if let Some(minio) = &config.minio
        && minio.is_configured()
    {
        instances.push(BackendInstance {
            backend: "minio".to_string(),
            name: "default".to_string(),
            role: BackendInstanceRole::ReadWrite,
            dsn: Some(minio.endpoint.clone()),
            dsn_env: None,
            ..BackendInstance::default()
        });
    }
    BackendInstanceConfig { instances }
}
