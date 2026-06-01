//! Redis cache executor for generic dispatch.

use redis::AsyncCommands;
use serde_json::{Value as Json, json};

use crate::runtime::executor_utils::build_probe;
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
            .map_err(|err| tonic::Status::unavailable(format!("redis connection failed: {err}")))?;
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

impl QueryExecutor for RedisExecutor {
    /// `{"operation":"get|mget|exists","key":"k","keys":["a"]}`.
    async fn query(&self, request_json: &str) -> Result<String, tonic::Status> {
        let spec: Json = serde_json::from_str(request_json).map_err(|err| {
            tonic::Status::invalid_argument(format!("invalid request json: {err}"))
        })?;
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
                    .ok_or_else(|| tonic::Status::invalid_argument("key is required"))?;
                let value: Option<Vec<u8>> = conn.get(key).await.map_err(|err| {
                    tonic::Status::unavailable(format!("redis GET failed: {err}"))
                })?;
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
                    .ok_or_else(|| tonic::Status::invalid_argument("keys must be an array"))?
                    .iter()
                    .filter_map(Json::as_str)
                    .collect::<Vec<_>>();
                let values: Vec<Option<Vec<u8>>> = conn.get(&keys).await.map_err(|err| {
                    tonic::Status::unavailable(format!("redis MGET failed: {err}"))
                })?;
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
                    .ok_or_else(|| tonic::Status::invalid_argument("key is required"))?;
                let exists: bool = conn.exists(key).await.map_err(|err| {
                    tonic::Status::unavailable(format!("redis EXISTS failed: {err}"))
                })?;
                Ok(json!({ "key": key, "exists": exists }).to_string())
            }
            other => Err(tonic::Status::invalid_argument(format!(
                "unsupported Redis query operation '{other}'"
            ))),
        }
    }
}

impl MutationExecutor for RedisExecutor {
    /// `{"operation":"set|delete|expire","key":"k","value":"v","ttl":60}`.
    async fn mutate(&self, request_json: &str) -> Result<String, tonic::Status> {
        let spec: Json = serde_json::from_str(request_json).map_err(|err| {
            tonic::Status::invalid_argument(format!("invalid request json: {err}"))
        })?;
        let operation = spec
            .get("operation")
            .and_then(Json::as_str)
            .unwrap_or("set");
        let key = spec
            .get("key")
            .and_then(Json::as_str)
            .ok_or_else(|| tonic::Status::invalid_argument("key is required"))?;
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
                    .ok_or_else(|| tonic::Status::invalid_argument("value is required"))?;
                if let Some(ttl) = spec.get("ttl").and_then(Json::as_u64) {
                    conn.set_ex::<_, _, ()>(key, value, ttl)
                        .await
                        .map_err(|err| {
                            tonic::Status::unavailable(format!("redis SETEX failed: {err}"))
                        })?;
                } else {
                    conn.set::<_, _, ()>(key, value).await.map_err(|err| {
                        tonic::Status::unavailable(format!("redis SET failed: {err}"))
                    })?;
                }
                Ok(json!({ "affected_rows": 1 }).to_string())
            }
            "delete" | "del" | "cache_delete" => {
                let deleted: u64 = conn.del(key).await.map_err(|err| {
                    tonic::Status::unavailable(format!("redis DEL failed: {err}"))
                })?;
                Ok(json!({ "affected_rows": deleted }).to_string())
            }
            "expire" => {
                let ttl = spec
                    .get("ttl")
                    .and_then(Json::as_i64)
                    .ok_or_else(|| tonic::Status::invalid_argument("ttl is required"))?;
                let changed: bool = conn.expire(key, ttl).await.map_err(|err| {
                    tonic::Status::unavailable(format!("redis EXPIRE failed: {err}"))
                })?;
                Ok(json!({ "affected_rows": i64::from(changed) }).to_string())
            }
            other => Err(tonic::Status::invalid_argument(format!(
                "unsupported Redis mutation operation '{other}'"
            ))),
        }
    }
}

impl SearchExecutor for RedisExecutor {
    async fn search(&self, _request_json: &str) -> Result<String, tonic::Status> {
        Err(tonic::Status::failed_precondition(
            "redis does not support vector search",
        ))
    }
}

impl ObjectExecutor for RedisExecutor {
    async fn get_object(&self, _request_json: &str) -> Result<Vec<u8>, tonic::Status> {
        Err(tonic::Status::failed_precondition(
            "redis is not an object store",
        ))
    }

    async fn put_object(
        &self,
        _request_json: &str,
        _bytes: Vec<u8>,
    ) -> Result<String, tonic::Status> {
        Err(tonic::Status::failed_precondition(
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
        Err(tonic::Status::failed_precondition(
            "redis does not expose resource lifecycle operations",
        ))
    }

    async fn drop_resource(&self, _resource_name: &str) -> Result<(), tonic::Status> {
        Err(tonic::Status::failed_precondition(
            "redis does not expose resource lifecycle operations",
        ))
    }

    async fn list_resources(&self) -> Result<Vec<String>, tonic::Status> {
        Err(tonic::Status::failed_precondition(
            "redis does not expose resource lifecycle operations",
        ))
    }
}

impl BackendExecutor for RedisExecutor {
    async fn transaction(&self, _request_json: &str) -> Result<String, tonic::Status> {
        Err(tonic::Status::failed_precondition(
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
