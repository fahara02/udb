//! Redis cache executor for generic dispatch.

use redis::AsyncCommands;
use serde_json::{Value as Json, json};

use crate::runtime::executor_utils::{
    backend_transport_status, build_probe, capability_status, invalid_argument_fields,
};
use crate::runtime::executors::{
    BackendExecutor, BackendHealth, BackendProbe, MutationExecutor, ObjectExecutor, QueryExecutor,
    ResourceAdminExecutor, SearchExecutor,
};

#[derive(Clone)]
pub(crate) struct RedisExecutor {
    client: redis::Client,
    // Lazily-established multiplexed connection, shared across requests and clones.
    // Cloning a `MultiplexedConnection` is cheap (one shared actor/socket), so each
    // call clones the cached handle instead of opening a fresh connection (#76).
    conn: std::sync::Arc<tokio::sync::OnceCell<redis::aio::MultiplexedConnection>>,
}

impl std::fmt::Debug for RedisExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisExecutor").finish_non_exhaustive()
    }
}

impl crate::runtime::backend_context::BackendContextEnforcer for RedisExecutor {
    fn backend_label(&self) -> &str {
        "redis"
    }

    fn enforce(
        &self,
        ctx: &crate::runtime::backend_context::AppliedContext,
    ) -> crate::runtime::backend_context::ContextEffect {
        // The Redis IR compiler emits keys of the form
        // `udb:{project}:{tenant}:<message>:<pk>`. The active context
        // is therefore enforced at key-construction time — a request
        // can't read or delete another tenant's key without going
        // around the broker.
        crate::runtime::backend_context::enforce_with_mechanism(
            ctx,
            "key namespace prefix udb:{project}:{tenant}:",
        )
    }
}

impl RedisExecutor {
    pub(crate) fn new(client: redis::Client) -> Self {
        Self {
            client,
            conn: std::sync::Arc::new(tokio::sync::OnceCell::new()),
        }
    }

    async fn connection(&self) -> Result<redis::aio::MultiplexedConnection, tonic::Status> {
        let conn = self
            .conn
            .get_or_try_init(|| self.client.get_multiplexed_async_connection())
            .await
            .map_err(|err| backend_transport_status("redis", "connection", err))?;
        Ok(conn.clone())
    }
}

impl BackendHealth for RedisExecutor {
    async fn ping(&self) -> Result<(), String> {
        let mut conn = self.connection().await.map_err(|err| err.to_string())?;
        redis::cmd("PING")
            .query_async::<String>(&mut conn)
            .await
            .map(|_| ())
            .map_err(|err| err.to_string())
    }
}

fn redis_invalid_field(
    field: impl Into<String>,
    description: impl Into<String>,
    message: impl Into<String>,
) -> tonic::Status {
    invalid_argument_fields(message, [(field.into(), description.into())])
}

fn invalid_redis_request_json_status(err: serde_json::Error) -> tonic::Status {
    redis_invalid_field(
        "request_json",
        "must be valid JSON for Redis generic dispatch",
        format!("invalid request json: {err}"),
    )
}

fn redis_required_field_status(field: &'static str, message: &'static str) -> tonic::Status {
    redis_invalid_field(
        field,
        format!("{field} is required for this Redis operation"),
        message,
    )
}

fn unsupported_redis_operation_status(kind: &'static str, operation: &str) -> tonic::Status {
    redis_invalid_field(
        "operation",
        format!("unsupported Redis {kind} operation"),
        format!("unsupported Redis {kind} operation '{operation}'"),
    )
}

impl QueryExecutor for RedisExecutor {
    /// `{"operation":"get|mget|exists","key":"k","keys":["a"]}`.
    async fn query(&self, request_json: &str) -> Result<String, tonic::Status> {
        let spec: Json =
            serde_json::from_str(request_json).map_err(invalid_redis_request_json_status)?;
        let operation = spec
            .get("operation")
            .and_then(Json::as_str)
            .unwrap_or("get");
        let mut conn = self.connection().await?;
        match operation {
            "get" | "cache_get" | "read_through" => {
                let key = spec
                    .get("key")
                    .and_then(Json::as_str)
                    .ok_or_else(|| redis_required_field_status("key", "key is required"))?;
                let value: Option<Vec<u8>> = conn
                    .get(key)
                    .await
                    .map_err(|err| backend_transport_status("redis", "GET", err))?;
                Ok(json!({
                    "key": key,
                    "hit": value.is_some(),
                    "value": value.and_then(|bytes| String::from_utf8(bytes).ok())
                })
                .to_string())
            }
            "mget" => {
                let keys = spec
                    .get("keys")
                    .and_then(Json::as_array)
                    .ok_or_else(|| {
                        redis_invalid_field(
                            "keys",
                            "must be an array for Redis mget",
                            "keys must be an array",
                        )
                    })?
                    .iter()
                    .filter_map(Json::as_str)
                    .collect::<Vec<_>>();
                let values: Vec<Option<Vec<u8>>> = conn
                    .get(&keys)
                    .await
                    .map_err(|err| backend_transport_status("redis", "MGET", err))?;
                Ok(json!({
                    "values": keys.into_iter().zip(values.into_iter()).map(|(key, value)| {
                        json!({
                            "key": key,
                            "hit": value.is_some(),
                            "value": value.and_then(|bytes| String::from_utf8(bytes).ok())
                        })
                    }).collect::<Vec<_>>()
                })
                .to_string())
            }
            "exists" => {
                let key = spec
                    .get("key")
                    .and_then(Json::as_str)
                    .ok_or_else(|| redis_required_field_status("key", "key is required"))?;
                let exists: bool = conn
                    .exists(key)
                    .await
                    .map_err(|err| backend_transport_status("redis", "EXISTS", err))?;
                Ok(json!({ "key": key, "exists": exists }).to_string())
            }
            "scan" | "cache_scan" => {
                let pattern = spec.get("pattern").and_then(Json::as_str).unwrap_or("*");
                let cursor = spec
                    .get("cursor")
                    .and_then(|value| {
                        value
                            .as_u64()
                            .or_else(|| value.as_str().and_then(|raw| raw.parse::<u64>().ok()))
                    })
                    .unwrap_or(0);
                let limit = spec
                    .get("limit")
                    .and_then(Json::as_u64)
                    .unwrap_or(10)
                    .max(1);
                let (next_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                    .arg(cursor)
                    .arg("MATCH")
                    .arg(pattern)
                    .arg("COUNT")
                    .arg(limit)
                    .query_async(&mut conn)
                    .await
                    .map_err(|err| backend_transport_status("redis", "SCAN", err))?;
                let values: Vec<Option<Vec<u8>>> = if keys.is_empty() {
                    Vec::new()
                } else {
                    conn.get(&keys)
                        .await
                        .map_err(|err| backend_transport_status("redis", "MGET after SCAN", err))?
                };
                let entries = keys
                    .into_iter()
                    .zip(values.into_iter())
                    .map(|(key, value)| {
                        json!({
                            "key": key,
                            "value": value.and_then(|bytes| String::from_utf8(bytes).ok())
                        })
                    })
                    .collect::<Vec<_>>();
                let next_page_token = if next_cursor == 0 {
                    String::new()
                } else {
                    next_cursor.to_string()
                };
                Ok(json!({
                    "entries": entries,
                    "next_page_token": next_page_token
                })
                .to_string())
            }
            other => Err(unsupported_redis_operation_status("query", other)),
        }
    }
}

impl MutationExecutor for RedisExecutor {
    /// `{"operation":"set|delete|expire","key":"k","value":"v","ttl":60}`.
    async fn mutate(&self, request_json: &str) -> Result<String, tonic::Status> {
        let spec: Json =
            serde_json::from_str(request_json).map_err(invalid_redis_request_json_status)?;
        let operation = spec
            .get("operation")
            .and_then(Json::as_str)
            .unwrap_or("set");
        let key = spec
            .get("key")
            .and_then(Json::as_str)
            .ok_or_else(|| redis_required_field_status("key", "key is required"))?;
        let mut conn = self.connection().await?;
        match operation {
            "set" | "cache_set" | "write_through" => {
                let value = spec
                    .get("value")
                    .map(|value| {
                        value
                            .as_str()
                            .map(ToString::to_string)
                            .unwrap_or_else(|| value.to_string())
                    })
                    .ok_or_else(|| redis_required_field_status("value", "value is required"))?;
                if let Some(ttl) = spec.get("ttl").and_then(Json::as_u64) {
                    conn.set_ex::<_, _, ()>(key, value, ttl)
                        .await
                        .map_err(|err| backend_transport_status("redis", "SETEX", err))?;
                } else {
                    conn.set::<_, _, ()>(key, value)
                        .await
                        .map_err(|err| backend_transport_status("redis", "SET", err))?;
                }
                Ok(json!({ "affected_rows": 1 }).to_string())
            }
            "delete" | "del" | "cache_delete" => {
                let deleted: u64 = conn
                    .del(key)
                    .await
                    .map_err(|err| backend_transport_status("redis", "DEL", err))?;
                Ok(json!({ "affected_rows": deleted }).to_string())
            }
            "expire" => {
                let ttl = spec
                    .get("ttl")
                    .and_then(Json::as_i64)
                    .ok_or_else(|| redis_required_field_status("ttl", "ttl is required"))?;
                let changed: bool = conn
                    .expire(key, ttl)
                    .await
                    .map_err(|err| backend_transport_status("redis", "EXPIRE", err))?;
                Ok(json!({ "affected_rows": i64::from(changed) }).to_string())
            }
            other => Err(unsupported_redis_operation_status("mutation", other)),
        }
    }
}

impl SearchExecutor for RedisExecutor {
    async fn search(&self, _request_json: &str) -> Result<String, tonic::Status> {
        Err(capability_status(
            "redis",
            "search",
            "vector_search",
            "redis does not support vector search",
        ))
    }
}

impl ObjectExecutor for RedisExecutor {
    async fn get_object(&self, _request_json: &str) -> Result<Vec<u8>, tonic::Status> {
        Err(capability_status(
            "redis",
            "get_object",
            "object_store",
            "redis is not an object store",
        ))
    }

    async fn put_object(
        &self,
        _request_json: &str,
        _bytes: Vec<u8>,
    ) -> Result<String, tonic::Status> {
        Err(capability_status(
            "redis",
            "put_object",
            "object_store",
            "redis is not an object store",
        ))
    }
}

impl ResourceAdminExecutor for RedisExecutor {
    async fn ensure_resource(
        &self,
        _resource_name: &str,
        _spec_json: &str,
    ) -> Result<(), tonic::Status> {
        Err(capability_status(
            "redis",
            "ensure_resource",
            "resource_lifecycle",
            "redis does not expose resource lifecycle operations",
        ))
    }

    async fn drop_resource(&self, _resource_name: &str) -> Result<(), tonic::Status> {
        Err(capability_status(
            "redis",
            "drop_resource",
            "resource_lifecycle",
            "redis does not expose resource lifecycle operations",
        ))
    }

    async fn list_resources(&self) -> Result<Vec<String>, tonic::Status> {
        Err(capability_status(
            "redis",
            "list_resources",
            "resource_lifecycle",
            "redis does not expose resource lifecycle operations",
        ))
    }
}

impl BackendExecutor for RedisExecutor {
    async fn transaction(&self, _request_json: &str) -> Result<String, tonic::Status> {
        Err(capability_status(
            "redis",
            "transaction",
            "transactions",
            "redis generic dispatch does not expose MULTI/EXEC transactions",
        ))
    }

    async fn probe(&self) -> Result<BackendProbe, tonic::Status> {
        Ok(build_probe(
            "redis",
            <Self as BackendHealth>::ping(self).await,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{ErrorDetail, ErrorKind};
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;
    use prost::Message as _;

    fn decode_detail(status: &tonic::Status) -> ErrorDetail {
        let raw = status
            .metadata()
            .get_bin(ERROR_DETAIL_METADATA_KEY)
            .expect("typed detail trailer is present");
        crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
    }

    fn assert_single_field(status: &tonic::Status, field: &str) {
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, field);
    }

    #[test]
    fn redis_request_json_validation_carries_field_violation() {
        let err = serde_json::from_str::<Json>("{")
            .map_err(invalid_redis_request_json_status)
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().starts_with("invalid request json:"));
        assert_single_field(&err, "request_json");
    }

    #[test]
    fn redis_required_field_validation_carries_field_violations() {
        let key = redis_required_field_status("key", "key is required");
        assert_eq!(key.message(), "key is required");
        assert_single_field(&key, "key");

        let value = redis_required_field_status("value", "value is required");
        assert_eq!(value.message(), "value is required");
        assert_single_field(&value, "value");

        let ttl = redis_required_field_status("ttl", "ttl is required");
        assert_eq!(ttl.message(), "ttl is required");
        assert_single_field(&ttl, "ttl");

        let keys = redis_invalid_field(
            "keys",
            "must be an array for Redis mget",
            "keys must be an array",
        );
        assert_eq!(keys.message(), "keys must be an array");
        assert_single_field(&keys, "keys");
    }

    #[test]
    fn redis_unsupported_operation_validation_carries_field_violation() {
        let query = unsupported_redis_operation_status("query", "bad");
        assert_eq!(query.message(), "unsupported Redis query operation 'bad'");
        assert_single_field(&query, "operation");

        let mutation = unsupported_redis_operation_status("mutation", "bad");
        assert_eq!(
            mutation.message(),
            "unsupported Redis mutation operation 'bad'"
        );
        assert_single_field(&mutation, "operation");
    }
}
