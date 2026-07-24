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
    let mut instances: Vec<RuntimeBackendInstance> = config
        .instances
        .iter()
        .map(|instance| runtime_backend_instance(instance, report, runtime))
        .collect();

    append_connected_runtime_instances(&mut instances, runtime);
    instances
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
        #[cfg(feature = "mysql")]
        "mysql" => {
            runtime.mysql_instances.contains_key(&instance.name)
                || (instance.name == "primary" && report.mysql_configured)
        }
        #[cfg(feature = "sqlite")]
        "sqlite" => {
            runtime.sqlite_instances.contains_key(&instance.name)
                || (instance.name == "primary" && report.sqlite_configured)
        }
        #[cfg(feature = "mssql")]
        "sqlserver" => {
            runtime.mssql_instances.contains_key(&instance.name)
                || (instance.name == "primary" && report.mssql_configured)
        }
        #[cfg(feature = "memcached")]
        "memcached" => {
            runtime.memcached_instances.contains_key(&instance.name)
                || (instance.name == "primary" && report.memcached_configured)
        }
        #[cfg(feature = "elasticsearch")]
        "elasticsearch" => {
            runtime.elasticsearch_instances.contains_key(&instance.name)
                || (instance.name == "primary" && report.elasticsearch_configured)
        }
        #[cfg(feature = "weaviate")]
        "weaviate" => {
            runtime.weaviate_instances.contains_key(&instance.name)
                || (instance.name == "primary" && report.weaviate_configured)
        }
        #[cfg(feature = "pinecone")]
        "pinecone" => {
            runtime.pinecone_instances.contains_key(&instance.name)
                || (instance.name == "primary" && report.pinecone_configured)
        }
        #[cfg(feature = "cassandra")]
        "cassandra" => {
            runtime.cassandra_instances.contains_key(&instance.name)
                || (instance.name == "primary" && report.cassandra_configured)
        }
        #[cfg(feature = "azureblob")]
        "azureblob" => {
            runtime.azureblob_instances.contains_key(&instance.name)
                || (instance.name == "primary" && report.azureblob_configured)
        }
        #[cfg(feature = "gcs")]
        "gcs" => {
            runtime.gcs_instances.contains_key(&instance.name)
                || (instance.name == "primary" && report.gcs_configured)
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

fn append_connected_runtime_instances(
    instances: &mut Vec<RuntimeBackendInstance>,
    runtime: &DataBrokerRuntime,
) {
    for name in runtime.pg_instances.keys() {
        append_connected_runtime_instance(instances, runtime, "postgres", name, "read_write");
    }

    #[cfg(feature = "redis")]
    for name in runtime.redis_instances.keys() {
        append_connected_runtime_instance(instances, runtime, "redis", name, "read_write");
    }

    #[cfg(feature = "qdrant")]
    for name in runtime.qdrant_instances.keys() {
        append_connected_runtime_instance(instances, runtime, "qdrant", name, "read_write");
    }

    #[cfg(feature = "s3")]
    for name in runtime.s3_instances.keys() {
        if !instances.iter().any(|instance| {
            matches!(instance.backend.as_str(), "minio" | "s3") && instance.name == *name
        }) {
            append_connected_runtime_instance(instances, runtime, "minio", name, "read_write");
        }
    }

    #[cfg(feature = "mongodb")]
    for name in runtime.mongodb_instances.keys() {
        append_connected_runtime_instance(instances, runtime, "mongodb", name, "read_write");
    }

    #[cfg(feature = "neo4j")]
    for name in runtime.neo4j_instances.keys() {
        append_connected_runtime_instance(instances, runtime, "neo4j", name, "read_write");
    }

    #[cfg(feature = "clickhouse")]
    for name in runtime.clickhouse_instances.keys() {
        append_connected_runtime_instance(instances, runtime, "clickhouse", name, "read");
    }

    #[cfg(feature = "mysql")]
    for name in runtime.mysql_instances.keys() {
        append_connected_runtime_instance(instances, runtime, "mysql", name, "read_write");
    }

    #[cfg(feature = "sqlite")]
    for name in runtime.sqlite_instances.keys() {
        append_connected_runtime_instance(instances, runtime, "sqlite", name, "read_write");
    }

    #[cfg(feature = "mssql")]
    for name in runtime.mssql_instances.keys() {
        append_connected_runtime_instance(instances, runtime, "sqlserver", name, "read_write");
    }

    #[cfg(feature = "memcached")]
    for name in runtime.memcached_instances.keys() {
        append_connected_runtime_instance(instances, runtime, "memcached", name, "read_write");
    }

    #[cfg(feature = "elasticsearch")]
    for name in runtime.elasticsearch_instances.keys() {
        append_connected_runtime_instance(instances, runtime, "elasticsearch", name, "read_write");
    }

    #[cfg(feature = "weaviate")]
    for name in runtime.weaviate_instances.keys() {
        append_connected_runtime_instance(instances, runtime, "weaviate", name, "read_write");
    }

    #[cfg(feature = "pinecone")]
    for name in runtime.pinecone_instances.keys() {
        append_connected_runtime_instance(instances, runtime, "pinecone", name, "read_write");
    }

    #[cfg(feature = "cassandra")]
    for name in runtime.cassandra_instances.keys() {
        append_connected_runtime_instance(instances, runtime, "cassandra", name, "read_write");
    }

    #[cfg(feature = "azureblob")]
    for name in runtime.azureblob_instances.keys() {
        append_connected_runtime_instance(instances, runtime, "azureblob", name, "read_write");
    }

    #[cfg(feature = "gcs")]
    for name in runtime.gcs_instances.keys() {
        append_connected_runtime_instance(instances, runtime, "gcs", name, "read_write");
    }
}

fn append_connected_runtime_instance(
    instances: &mut Vec<RuntimeBackendInstance>,
    runtime: &DataBrokerRuntime,
    backend: &str,
    name: &str,
    role: &str,
) {
    if instances
        .iter()
        .any(|instance| instance.backend == backend && instance.name == name)
    {
        return;
    }
    let circuit_open = !runtime.circuit_breaker_allows(backend, Some(name));
    instances.push(RuntimeBackendInstance {
        name: name.to_string(),
        backend: backend.to_string(),
        role: role.to_string(),
        enabled: true,
        configured: true,
        connected: true,
        read_weight: 1,
        write_weight: 1,
        dsn_env: None,
        labels: HashMap::new(),
        capabilities: Vec::new(),
        healthy: !circuit_open,
        circuit_open,
    });
}

pub(crate) fn reconcile_dispatch_factories(
    instances: &mut [RuntimeBackendInstance],
    runtime: &DataBrokerRuntime,
    warnings: &mut Vec<String>,
) {
    for instance in instances
        .iter_mut()
        .filter(|instance| instance.enabled && instance.connected)
    {
        let Some(kind) = crate::backend::BackendKind::from_store_kind("", &instance.backend)
            .or_else(|| crate::backend::BackendKind::from_token(&instance.backend))
        else {
            disconnect_dispatch_instance(
                instance,
                warnings,
                format!("unknown backend '{}'", instance.backend),
            );
            continue;
        };
        if crate::runtime::executors::handle::dispatch_factory_for(&kind).is_none() {
            disconnect_dispatch_instance(
                instance,
                warnings,
                format!(
                    "backend '{}' has no generic-dispatch factory",
                    instance.backend
                ),
            );
            continue;
        }
        let write = !instance.role.eq_ignore_ascii_case("read");
        if let Err(err) = runtime.resolve_dispatch_executor(
            &instance.backend,
            Some(&instance.name),
            write,
            tonic::Code::FailedPrecondition,
            None,
        ) {
            disconnect_dispatch_instance(
                instance,
                warnings,
                format!("dispatch factory could not build executor: {err}"),
            );
        }
    }
}

fn disconnect_dispatch_instance(
    instance: &mut RuntimeBackendInstance,
    warnings: &mut Vec<String>,
    reason: String,
) {
    instance.connected = false;
    instance.healthy = false;
    warnings.push(format!(
        "backend executor '{}:{}' not registered: {reason}",
        instance.backend, instance.name
    ));
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
        "mysql" => report.mysql_configured,
        "sqlite" => report.sqlite_configured,
        "mssql" | "sqlserver" => report.mssql_configured,
        "memcached" => report.memcached_configured,
        "elasticsearch" => report.elasticsearch_configured,
        "weaviate" => report.weaviate_configured,
        "pinecone" => report.pinecone_configured,
        "cassandra" => report.cassandra_configured,
        "azureblob" => report.azureblob_configured,
        "gcs" => report.gcs_configured,
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
    serde_json::from_str(request_json).map_err(|err| {
        core_helper_invalid_field(
            "spec_json",
            "must be valid JSON",
            format!("invalid spec_json: {err}"),
        )
    })
}

fn core_helper_invalid_field(
    field: impl Into<String>,
    description: impl Into<String>,
    message: impl Into<String>,
) -> tonic::Status {
    crate::runtime::executor_utils::invalid_argument_fields(
        message,
        [(field.into(), description.into())],
    )
}

fn core_helper_failed_precondition_field(
    field: impl Into<String>,
    description: impl Into<String>,
    message: impl Into<String>,
) -> tonic::Status {
    crate::runtime::executor_utils::failed_precondition_fields(
        message,
        [(field.into(), description.into())],
    )
}

// `json_required_str`, `json_required_f32_vec`, `json_i32`, and `json_bool` now
// live in `executor_utils` (imported via `use crate::runtime::executor_utils::*`) so the
// per-backend executors can share them.

// `object_bytes_from_json` now lives in `executor_utils` (imported via the glob).

pub(crate) fn dispatch_params(value: &JsonValue) -> Result<Vec<JsonValue>, tonic::Status> {
    match value.get("params").or_else(|| value.get("parameters")) {
        Some(JsonValue::Array(params)) => Ok(params.clone()),
        Some(_) => Err(core_helper_invalid_field(
            "params",
            "must be an array",
            "params/parameters must be an array",
        )),
        None => Ok(Vec::new()),
    }
}

pub(crate) fn dispatch_param_types(
    value: &JsonValue,
) -> Result<Option<Vec<String>>, tonic::Status> {
    match value.get("param_types") {
        Some(JsonValue::Array(types)) => types
            .iter()
            .map(|value| {
                value.as_str().map(str::to_string).ok_or_else(|| {
                    core_helper_invalid_field(
                        "param_types",
                        "entries must be strings",
                        "param_types entries must be strings",
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()
            .map(Some),
        Some(_) => Err(core_helper_invalid_field(
            "param_types",
            "must be an array",
            "param_types must be an array",
        )),
        None => Ok(None),
    }
}

pub(crate) fn validate_single_statement(sql: &str) -> Result<(), tonic::Status> {
    let trimmed = sql.trim();
    if trimmed.is_empty() {
        return Err(core_helper_invalid_field(
            "sql",
            "must be non-empty",
            "sql is required",
        ));
    }
    let semicolon_count = trimmed.chars().filter(|ch| *ch == ';').count();
    if semicolon_count > 1 || (semicolon_count == 1 && !trimmed.ends_with(';')) {
        return Err(core_helper_invalid_field(
            "sql",
            "must contain exactly one statement",
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
        Err(core_helper_failed_precondition_field(
            "sql",
            "must start with SELECT, WITH, SHOW, or EXPLAIN",
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
        Err(core_helper_failed_precondition_field(
            "sql",
            "must start with INSERT, UPDATE, or DELETE",
            "generic PostgreSQL mutate allows only INSERT, UPDATE, or DELETE",
        ))
    }
}

/// Backend-neutral read-SQL guard for generic dispatch on MySQL/SQLite/MSSQL.
/// Mirrors [`validate_pg_read_sql`] (single statement + read-only leading verb)
/// without the Postgres-specific wording so the other SQL backends enforce the
/// same statement-type allowlist Postgres does.
pub(crate) fn validate_read_sql(sql: &str) -> Result<(), tonic::Status> {
    validate_single_statement(sql)?;
    let first = sql
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .trim_end_matches(';')
        .to_ascii_lowercase();
    if matches!(
        first.as_str(),
        "select" | "with" | "show" | "explain" | "pragma"
    ) {
        Ok(())
    } else {
        Err(core_helper_failed_precondition_field(
            "sql",
            "must start with SELECT, WITH, SHOW, EXPLAIN, or PRAGMA",
            "generic query allows only SELECT, WITH, SHOW, EXPLAIN, or PRAGMA",
        ))
    }
}

/// Backend-neutral mutation-SQL guard for generic dispatch on MySQL/SQLite/MSSQL.
/// Mirrors [`validate_pg_mutation_sql`] but also accepts `REPLACE` (MySQL/SQLite
/// upsert verb).
pub(crate) fn validate_mutation_sql(sql: &str) -> Result<(), tonic::Status> {
    validate_single_statement(sql)?;
    let first = sql
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .trim_end_matches(';')
        .to_ascii_lowercase();
    if matches!(first.as_str(), "insert" | "update" | "delete" | "replace") {
        Ok(())
    } else {
        Err(core_helper_failed_precondition_field(
            "sql",
            "must start with INSERT, UPDATE, DELETE, or REPLACE",
            "generic mutate allows only INSERT, UPDATE, DELETE, or REPLACE",
        ))
    }
}

pub(crate) fn bind_generic_pg_params<'q>(
    mut query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    params: &'q [JsonValue],
) -> sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
    for value in params {
        query = bind_generic_pg_param(query, value);
    }
    query
}

pub(crate) fn bind_typed_generic_pg_params<'q>(
    mut query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    params: &'q [JsonValue],
    param_types: Option<&[String]>,
) -> Result<sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>, tonic::Status> {
    let Some(param_types) = param_types else {
        return Ok(bind_generic_pg_params(query, params));
    };
    if param_types.len() != params.len() {
        return Err(core_helper_invalid_field(
            "param_types",
            "length must match params length",
            "param_types length must match params length",
        ));
    }
    for (value, param_type) in params.iter().zip(param_types) {
        query = match param_type.as_str() {
            "array_string" => {
                let values = json_array_values(value, "array_string")?
                    .iter()
                    .map(|item| {
                        item.as_str().map(strip_nul).ok_or_else(|| {
                            core_helper_invalid_field(
                                "params",
                                "array_string params must contain only strings",
                                "array_string params must contain only strings",
                            )
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                query.bind(values)
            }
            "array_int" => {
                let values = json_array_values(value, "array_int")?
                    .iter()
                    .map(|item| {
                        item.as_i64().ok_or_else(|| {
                            core_helper_invalid_field(
                                "params",
                                "array_int params must contain only integers",
                                "array_int params must contain only integers",
                            )
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                query.bind(values)
            }
            "array_float" => {
                let values = json_array_values(value, "array_float")?
                    .iter()
                    .map(|item| {
                        item.as_f64().ok_or_else(|| {
                            core_helper_invalid_field(
                                "params",
                                "array_float params must contain only numbers",
                                "array_float params must contain only numbers",
                            )
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                query.bind(values)
            }
            "array_bool" => {
                let values = json_array_values(value, "array_bool")?
                    .iter()
                    .map(|item| {
                        item.as_bool().ok_or_else(|| {
                            core_helper_invalid_field(
                                "params",
                                "array_bool params must contain only booleans",
                                "array_bool params must contain only booleans",
                            )
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                query.bind(values)
            }
            "json" => query.bind(sqlx::types::Json(strip_nul_json(value))),
            "timestamptz" => {
                // Parse RFC-3339 text back into a real DateTime<Utc> so sqlx
                // binds timestamptz. A typed NULL must bind as a nullable
                // timestamptz too; otherwise compiler-emitted `$n::TIMESTAMPTZ`
                // placeholders can still receive an untyped text/null bind.
                match value {
                    JsonValue::Null => query.bind(Option::<chrono::DateTime<chrono::Utc>>::None),
                    JsonValue::String(raw) if raw.trim().is_empty() => {
                        query.bind(Option::<chrono::DateTime<chrono::Utc>>::None)
                    }
                    JsonValue::String(raw) => {
                        let dt = chrono::DateTime::parse_from_rfc3339(raw).map_err(|err| {
                            core_helper_invalid_field(
                                "params",
                                "timestamptz params must be RFC3339 strings",
                                format!("timestamptz params must be RFC3339 strings: {err}"),
                            )
                        })?;
                        query.bind(dt.with_timezone(&chrono::Utc))
                    }
                    _ => {
                        return Err(core_helper_invalid_field(
                            "params",
                            "timestamptz params must be strings or null",
                            "timestamptz params must be strings or null",
                        ));
                    }
                }
            }
            "uuid" => match value {
                JsonValue::Null => query.bind(Option::<uuid::Uuid>::None),
                JsonValue::String(raw) if raw.trim().is_empty() => {
                    query.bind(Option::<uuid::Uuid>::None)
                }
                JsonValue::String(raw) => {
                    let parsed = uuid::Uuid::parse_str(raw).map_err(|err| {
                        core_helper_invalid_field(
                            "params",
                            "uuid params must be UUID strings",
                            format!("uuid params must be UUID strings: {err}"),
                        )
                    })?;
                    query.bind(parsed)
                }
                _ => {
                    return Err(core_helper_invalid_field(
                        "params",
                        "uuid params must be strings or null",
                        "uuid params must be strings or null",
                    ));
                }
            },
            _ => bind_generic_pg_param(query, value),
        };
    }
    Ok(query)
}

/// A NUL (`0x00`) byte can never be stored in a Postgres `text`/`varchar`/
/// `text[]`/`json(b)` value. Strip NULs at the bind edge so generic dispatch
/// cannot fail a whole statement with Postgres' UTF-8 NUL rejection.
fn strip_nul(s: &str) -> String {
    if s.contains('\u{0}') {
        s.replace('\u{0}', "")
    } else {
        s.to_string()
    }
}

/// Recursively strip NUL bytes from every string in a JSON value (for JSONB binds).
fn strip_nul_json(value: &JsonValue) -> JsonValue {
    match value {
        JsonValue::String(s) if s.contains('\u{0}') => JsonValue::String(s.replace('\u{0}', "")),
        JsonValue::Array(items) => JsonValue::Array(items.iter().map(strip_nul_json).collect()),
        JsonValue::Object(map) => JsonValue::Object(
            map.iter()
                .map(|(k, v)| (k.clone(), strip_nul_json(v)))
                .collect(),
        ),
        other => other.clone(),
    }
}

fn bind_generic_pg_param<'q>(
    query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    value: &'q JsonValue,
) -> sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
    match value {
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
        JsonValue::String(value) => query.bind(strip_nul(value)),
        JsonValue::Array(_) | JsonValue::Object(_) => {
            query.bind(sqlx::types::Json(strip_nul_json(value)))
        }
    }
}

fn json_array_values<'a>(
    value: &'a JsonValue,
    param_type: &str,
) -> Result<&'a Vec<JsonValue>, tonic::Status> {
    value.as_array().ok_or_else(|| {
        core_helper_invalid_field(
            "params",
            format!("{param_type} params must be arrays"),
            format!("{param_type} params must be arrays"),
        )
    })
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
        crate::runtime::config::DEFAULT_DB_ACQUIRE_TIMEOUT_SECS
    });
    let idle_timeout = Duration::from_secs(if config.conn_max_idle_secs > 0 {
        config.conn_max_idle_secs
    } else {
        crate::runtime::config::DEFAULT_DB_IDLE_TIMEOUT_SECS
    });
    let max_lifetime = Duration::from_secs(if config.conn_max_lifetime_secs > 0 {
        config.conn_max_lifetime_secs
    } else {
        crate::runtime::config::DEFAULT_DB_MAX_LIFETIME_SECS
    });
    let connection_string = append_application_name(dsn, app_name);
    let min_connections = if config.min_connections > 0 {
        config.min_connections as u32
    } else {
        crate::runtime::config::DEFAULT_DB_MIN_CONNECTIONS as u32
    };
    let max_connections = if config.max_open_conns > 0 {
        config.max_open_conns as u32
    } else {
        crate::runtime::config::DEFAULT_DB_MAX_OPEN_CONNS as u32
    };
    // L3: per-connection prepared-statement cache. sqlx defaults to 100; UDB
    // emits many distinct parameterized SELECT/UPSERT strings across message
    // types, so raise the cap so repeat reads are parsed/planned ONCE per
    // physical connection (pooled-driver parity; pairs with the broker
    // compiled-plan cache). Cached statements are kept for the connection's
    // lifetime (sqlx does not DEALLOCATE unless the cache overflows), so warm
    // pooled connections never re-parse a hot statement.
    const PG_STATEMENT_CACHE_CAPACITY: usize = 512;
    let connect_options = connection_string
        .parse::<sqlx::postgres::PgConnectOptions>()
        .map_err(|err| err.to_string())?
        .statement_cache_capacity(PG_STATEMENT_CACHE_CAPACITY);
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
        .connect_with(connect_options)
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

#[cfg(test)]
pub(crate) fn live_postgres_dsn_for_tests() -> Option<String> {
    std::env::var("UDB_PG_DSN")
        .ok()
        .or_else(|| std::env::var("DATABASE_URL").ok())
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

#[cfg(feature = "mssql")]
pub(crate) fn mssql_executor_from_instance(
    instance: &BackendInstance,
) -> Option<crate::runtime::executors::mssql::MssqlClient> {
    instance
        .resolve_dsn()
        .map(crate::runtime::executors::mssql::MssqlClient::new)
}

#[cfg(feature = "cassandra")]
pub(crate) async fn cassandra_executor_from_instance(
    instance: &BackendInstance,
) -> Result<Option<crate::runtime::executors::cassandra::CassandraClient>, String> {
    let Some(dsn) = instance.resolve_dsn() else {
        return Ok(None);
    };
    crate::runtime::executors::cassandra::CassandraClient::connect(&dsn)
        .await
        .map(Some)
        .map_err(|err| err.to_string())
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

#[cfg(test)]
mod generic_dispatch_validation_tests {
    use super::*;
    use crate::proto::{ErrorDetail, ErrorKind};
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;
    use serde_json::json;

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

    fn assert_failed_precondition_field_violation(
        status: &tonic::Status,
        field: &str,
        description: &str,
    ) {
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert!(!detail.retryable);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, field);
        assert_eq!(detail.field_violations[0].description, description);
    }

    fn expect_status<T>(result: Result<T, tonic::Status>, context: &str) -> tonic::Status {
        match result {
            Ok(_) => panic!("{context}"),
            Err(status) => status,
        }
    }

    #[test]
    fn generic_dispatch_json_and_sql_validation_carry_field_violations() {
        let invalid_spec = parse_dispatch_json("{").unwrap_err();
        assert!(
            invalid_spec.message().starts_with("invalid spec_json:"),
            "unexpected message: {}",
            invalid_spec.message()
        );
        assert_single_field_violation(&invalid_spec, "spec_json", "must be valid JSON");

        let params = dispatch_params(&json!({ "params": "not-array" })).unwrap_err();
        assert_eq!(params.message(), "params/parameters must be an array");
        assert_single_field_violation(&params, "params", "must be an array");

        let param_types = dispatch_param_types(&json!({ "param_types": [1] })).unwrap_err();
        assert_eq!(param_types.message(), "param_types entries must be strings");
        assert_single_field_violation(&param_types, "param_types", "entries must be strings");

        let param_types_shape =
            dispatch_param_types(&json!({ "param_types": "uuid" })).unwrap_err();
        assert_eq!(param_types_shape.message(), "param_types must be an array");
        assert_single_field_violation(&param_types_shape, "param_types", "must be an array");

        let empty_sql = validate_single_statement("").unwrap_err();
        assert_eq!(empty_sql.message(), "sql is required");
        assert_single_field_violation(&empty_sql, "sql", "must be non-empty");

        let multi_sql = validate_single_statement("select 1; select 2").unwrap_err();
        assert_eq!(
            multi_sql.message(),
            "generic PostgreSQL dispatch accepts exactly one statement"
        );
        assert_single_field_violation(&multi_sql, "sql", "must contain exactly one statement");

        let pg_read_verb = validate_pg_read_sql("delete from customers").unwrap_err();
        assert_eq!(
            pg_read_verb.message(),
            "generic PostgreSQL query allows only SELECT, WITH, SHOW, or EXPLAIN"
        );
        assert_failed_precondition_field_violation(
            &pg_read_verb,
            "sql",
            "must start with SELECT, WITH, SHOW, or EXPLAIN",
        );

        let pg_mutation_verb = validate_pg_mutation_sql("select 1").unwrap_err();
        assert_eq!(
            pg_mutation_verb.message(),
            "generic PostgreSQL mutate allows only INSERT, UPDATE, or DELETE"
        );
        assert_failed_precondition_field_violation(
            &pg_mutation_verb,
            "sql",
            "must start with INSERT, UPDATE, or DELETE",
        );

        let read_verb = validate_read_sql("delete from customers").unwrap_err();
        assert_eq!(
            read_verb.message(),
            "generic query allows only SELECT, WITH, SHOW, EXPLAIN, or PRAGMA"
        );
        assert_failed_precondition_field_violation(
            &read_verb,
            "sql",
            "must start with SELECT, WITH, SHOW, EXPLAIN, or PRAGMA",
        );

        let mutation_verb = validate_mutation_sql("select 1").unwrap_err();
        assert_eq!(
            mutation_verb.message(),
            "generic mutate allows only INSERT, UPDATE, DELETE, or REPLACE"
        );
        assert_failed_precondition_field_violation(
            &mutation_verb,
            "sql",
            "must start with INSERT, UPDATE, DELETE, or REPLACE",
        );
    }

    #[test]
    fn typed_generic_pg_param_validation_carries_field_violations() {
        let array_shape = json_array_values(&json!("not-array"), "array_string").unwrap_err();
        assert_eq!(array_shape.message(), "array_string params must be arrays");
        assert_single_field_violation(&array_shape, "params", "array_string params must be arrays");

        let len_mismatch = expect_status(
            bind_typed_generic_pg_params(
                sqlx::query("SELECT $1"),
                &[json!("value")],
                Some(&["uuid".to_string(), "uuid".to_string()]),
            ),
            "param_types length mismatch must fail",
        );
        assert_eq!(
            len_mismatch.message(),
            "param_types length must match params length"
        );
        assert_single_field_violation(
            &len_mismatch,
            "param_types",
            "length must match params length",
        );

        let bad_uuid = expect_status(
            bind_typed_generic_pg_params(
                sqlx::query("SELECT $1"),
                &[json!("not-a-uuid")],
                Some(&["uuid".to_string()]),
            ),
            "invalid uuid param must fail",
        );
        assert!(
            bad_uuid
                .message()
                .starts_with("uuid params must be UUID strings:"),
            "unexpected message: {}",
            bad_uuid.message()
        );
        assert_single_field_violation(&bad_uuid, "params", "uuid params must be UUID strings");

        let bad_timestamp = expect_status(
            bind_typed_generic_pg_params(
                sqlx::query("SELECT $1"),
                &[json!("not-a-timestamp")],
                Some(&["timestamptz".to_string()]),
            ),
            "invalid timestamptz param must fail",
        );
        assert!(
            bad_timestamp
                .message()
                .starts_with("timestamptz params must be RFC3339 strings:"),
            "unexpected message: {}",
            bad_timestamp.message()
        );
        assert_single_field_violation(
            &bad_timestamp,
            "params",
            "timestamptz params must be RFC3339 strings",
        );
    }
}

#[cfg(all(test, feature = "mssql"))]
mod b4_mssql_helper_tests {
    use super::*;

    #[test]
    fn mssql_helper_builds_lazy_client_from_inline_dsn() {
        let configured = BackendInstance {
            backend: "mssql".to_string(),
            name: "primary".to_string(),
            dsn: Some("Server=localhost,1433;Database=udb;User=sa;Password=x;".to_string()),
            dsn_env: None,
            ..BackendInstance::default()
        };
        assert!(mssql_executor_from_instance(&configured).is_some());

        let unconfigured = BackendInstance {
            backend: "mssql".to_string(),
            name: "secondary".to_string(),
            dsn: None,
            dsn_env: None,
            ..BackendInstance::default()
        };
        assert!(mssql_executor_from_instance(&unconfigured).is_none());
    }
}

// ── Compiled-IR Postgres parameter helpers ────────────────────────────────────
// Shared by the GenericDispatch compiled-dispatch path (handlers_data) and the
// 2.4 bridged data-plane emitter below: LogicalValue params are converted to
// JSON wire values plus per-parameter type hints so `bind_typed_generic_pg_params`
// restores typed binds (timestamptz/uuid/json/arrays) instead of text.

pub(crate) fn logical_value_to_json(value: &crate::ir::value::LogicalValue) -> serde_json::Value {
    use crate::ir::value::LogicalValue;
    match value {
        LogicalValue::Null => serde_json::Value::Null,
        LogicalValue::Bool(value) => serde_json::Value::Bool(*value),
        LogicalValue::Int(value) => serde_json::json!(value),
        LogicalValue::Float(value) => serde_json::Number::from_f64(*value)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        LogicalValue::String(value) => serde_json::Value::String(value.clone()),
        LogicalValue::Bytes(value) => {
            use base64::{Engine as _, engine::general_purpose::STANDARD as B64};
            serde_json::Value::String(format!("base64:{}", B64.encode(value)))
        }
        LogicalValue::Timestamp(value) => serde_json::Value::String(
            // Canonical wire form: `Z` suffix, not `+00:00` — chrono's plain
            // `to_rfc3339()` renders the offset form, which every SDK decoder
            // has to special-case. AutoSi keeps the stored sub-second precision.
            value.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true),
        ),
        LogicalValue::Json(value) => value.clone(),
        LogicalValue::Array(values) => {
            serde_json::Value::Array(values.iter().map(logical_value_to_json).collect())
        }
    }
}

fn logical_value_param_type(value: &crate::ir::value::LogicalValue) -> &'static str {
    use crate::ir::value::LogicalValue;
    match value {
        LogicalValue::Json(_) => "json",
        // A `Timestamp` renders to an RFC-3339 *string*; without this hint the
        // executor would bind it as `text`, which Postgres refuses to coerce
        // into a `timestamptz` column on INSERT ("column … is of type timestamp
        // with time zone but expression is of type text"). Type it so the bind
        // path parses it back to a real `DateTime<Utc>`.
        LogicalValue::Timestamp(_) => "timestamptz",
        LogicalValue::Array(values) => match values
            .iter()
            .find(|value| !matches!(value, LogicalValue::Null))
        {
            Some(LogicalValue::String(_)) => "array_string",
            Some(LogicalValue::Int(_)) => "array_int",
            Some(LogicalValue::Float(_)) => "array_float",
            Some(LogicalValue::Bool(_)) => "array_bool",
            _ => "json",
        },
        _ => "",
    }
}

pub(crate) fn postgres_param_types(
    statement: &str,
    params: &[crate::ir::value::LogicalValue],
) -> Vec<&'static str> {
    params
        .iter()
        .enumerate()
        .map(|(idx, value)| {
            let value_type = logical_value_param_type(value);
            if value_type.is_empty() {
                postgres_placeholder_cast_type(statement, idx + 1).unwrap_or("")
            } else {
                value_type
            }
        })
        .collect()
}

fn postgres_placeholder_cast_type(statement: &str, position: usize) -> Option<&'static str> {
    let marker = format!("${position}::");
    let (_, tail) = statement.split_once(&marker)?;
    let lower = tail.trim_start().to_ascii_lowercase();
    if lower.starts_with("timestamp with time zone") || lower.starts_with("timestamptz") {
        Some("timestamptz")
    } else if lower.starts_with("uuid") {
        Some("uuid")
    } else {
        None
    }
}

// ── 2.4 merge seam: bridged neutral-IR emitter for the Postgres data plane ────
// The data-plane SELECT/UPSERT/DELETE wrappers keep every value-add (plan-error
// validation, plan cache, scope/purpose checks, PII projection exclusion,
// encryption, dedup, outbox/projection enqueue, audit/cache policy) and only the
// SQL emission is swapped: when the planner request lowers to neutral IR, the
// shared Postgres compiler renders the statement (live row equivalence is pinned
// by `postgres_data_plane_planner_and_bridged_ir_match_live_rows`). Requests the
// neutral IR cannot represent (planner-only JSONB/full-text operators,
// alternate-unique DO NOTHING) fall back to the planner SQL — merge, not delete.

pub(crate) struct BridgedPgStatement {
    pub(crate) sql: String,
    pub(crate) params: Vec<JsonValue>,
    pub(crate) param_types: Vec<String>,
}

/// Kill switch for the bridged emitter, resolved once. Default ON; set
/// `UDB_PG_BRIDGED_EMITTER=0|false|off` to restore planner-emitted SQL for the
/// full surface (the fallback path that remains in place for planner-only shapes).
pub(crate) fn pg_bridged_emitter_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        !matches!(
            std::env::var("UDB_PG_BRIDGED_EMITTER")
                .unwrap_or_default()
                .trim()
                .to_ascii_lowercase()
                .as_str(),
            "0" | "false" | "off"
        )
    })
}

fn compile_bridged_postgres_statement(
    manifest: &CatalogManifest,
    context: &crate::RequestContext,
    operation: crate::ir::compile::CompileOperation<'_>,
) -> Option<BridgedPgStatement> {
    use crate::ir::compile::{CompileContext, CompiledRendering, compile_for_backend};
    let compile_ctx = CompileContext::new(manifest)
        .with_tenant(&context.tenant_id)
        .with_project(&context.project_id);
    match compile_for_backend(
        &crate::backend::BackendKind::Postgres,
        operation,
        &compile_ctx,
    ) {
        Some(Ok(CompiledRendering::Sql {
            backend,
            statement,
            params,
        })) if backend == crate::backend::BackendKind::Postgres => {
            let param_types = postgres_param_types(&statement, &params)
                .into_iter()
                .map(str::to_string)
                .collect();
            let params = params.iter().map(logical_value_to_json).collect();
            Some(BridgedPgStatement {
                sql: statement,
                params,
                param_types,
            })
        }
        Some(Err(err)) => {
            tracing::debug!(error = %err, "bridged Postgres IR compile fell back to planner SQL");
            None
        }
        _ => None,
    }
}

/// Bridged emission for the data-plane SELECT path. `None` = use planner SQL.
pub(crate) fn bridged_pg_select_statement(
    manifest: &CatalogManifest,
    request: &crate::planning::broker::SelectPlanRequest,
) -> Option<BridgedPgStatement> {
    if !pg_bridged_emitter_enabled() {
        return None;
    }
    let read = crate::planning::broker::build_select_logical_read(manifest, request).ok()?;
    compile_bridged_postgres_statement(
        manifest,
        &request.context,
        crate::ir::compile::CompileOperation::Read(&read),
    )
}

/// Bridged emission for the data-plane UPSERT path. The caller passes the
/// plan request whose `record` is ALREADY key-normalized and encrypted, so the
/// compiled parameter values match what the planner path would bind. `None` =
/// use planner SQL.
pub(crate) fn bridged_pg_upsert_statement(
    manifest: &CatalogManifest,
    request: &crate::planning::broker::UpsertPlanRequest,
) -> Option<BridgedPgStatement> {
    if !pg_bridged_emitter_enabled() {
        return None;
    }
    let write = crate::planning::broker::build_upsert_logical_write(manifest, request).ok()?;
    compile_bridged_postgres_statement(
        manifest,
        &request.context,
        crate::ir::compile::CompileOperation::Write(&write),
    )
}

/// Bridged emission for the data-plane DELETE path. `None` = use planner SQL.
pub(crate) fn bridged_pg_delete_statement(
    manifest: &CatalogManifest,
    request: &crate::planning::broker::DeletePlanRequest,
) -> Option<BridgedPgStatement> {
    if !pg_bridged_emitter_enabled() {
        return None;
    }
    let delete = crate::planning::broker::build_delete_logical_delete(manifest, request).ok()?;
    compile_bridged_postgres_statement(
        manifest,
        &request.context,
        crate::ir::compile::CompileOperation::Delete(&delete),
    )
}
