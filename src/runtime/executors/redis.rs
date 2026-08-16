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
    // Lazily-established reconnecting connection, shared across requests and clones.
    // The manager returns the first failed command without replaying it, then replaces
    // the dead transport in the background so a later request can retry safely.
    conn: std::sync::Arc<tokio::sync::OnceCell<redis::aio::ConnectionManager>>,
    // Tenant/project key namespace `udb:{project}:{tenant}:` derived from the
    // request context by the dispatch factory. When present, every raw key and
    // SCAN pattern is forced under it so a raw-dispatch caller cannot read, scan,
    // or mutate another tenant's keys (C6). `None` only on the context-free path
    // (health probes / admin tooling).
    namespace: Option<String>,
}

/// Build the `udb:{project}:{tenant}:` key namespace that the Redis IR compiler
/// (`build_key`) writes under. Empty tenant/project fall back to `default`, so a
/// context always yields a concrete, isolating prefix.
pub(crate) fn redis_context_namespace(project_id: &str, tenant_id: &str) -> String {
    let project = if project_id.trim().is_empty() {
        "default"
    } else {
        project_id
    };
    let tenant = if tenant_id.trim().is_empty() {
        "default"
    } else {
        tenant_id
    };
    format!("udb:{project}:{tenant}:")
}

/// Force a caller SCAN pattern under the tenant/project namespace so it cannot
/// enumerate another tenant's keys (C6). A caller pattern that does not already
/// sit under the namespace is re-anchored beneath it (so a cross-tenant pattern
/// like `udb:other:*` becomes `udb:me:...udb:other:*` and matches nothing). With
/// no namespace (context-free path) the pattern is returned unchanged.
pub(crate) fn effective_scan_pattern(namespace: Option<&str>, caller_pattern: &str) -> String {
    match namespace {
        Some(ns) if !ns.is_empty() => {
            let suffix = caller_pattern.strip_prefix(ns).unwrap_or(caller_pattern);
            let suffix = if suffix.is_empty() { "*" } else { suffix };
            format!("{ns}{suffix}")
        }
        _ => caller_pattern.to_string(),
    }
}

/// Force a raw key under the tenant/project namespace (C6). A key already under
/// the namespace is left as-is (idempotent); any other key is prefixed so it
/// cannot address another tenant's namespace.
pub(crate) fn scope_redis_key(namespace: Option<&str>, key: &str) -> String {
    match namespace {
        Some(ns) if !ns.is_empty() => {
            if key.starts_with(ns) {
                key.to_string()
            } else {
                format!("{ns}{key}")
            }
        }
        _ => key.to_string(),
    }
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
            namespace: None,
        }
    }

    /// Attach the tenant/project key namespace derived from the request context.
    /// The dispatch factory calls this so raw KV/scan ops are tenant-scoped (C6).
    pub(crate) fn with_namespace(mut self, namespace: Option<String>) -> Self {
        self.namespace = namespace.filter(|ns| !ns.is_empty());
        self
    }

    fn namespace(&self) -> Option<&str> {
        self.namespace.as_deref()
    }

    async fn connection(&self) -> Result<redis::aio::ConnectionManager, tonic::Status> {
        let conn = self
            .conn
            .get_or_try_init(|| redis::aio::ConnectionManager::new(self.client.clone()))
            .await
            .map_err(|err| backend_transport_status("redis", "connection", err))?;
        Ok(conn.clone())
    }

    #[cfg(test)]
    pub(crate) async fn live_connection_id(&self) -> Result<i64, tonic::Status> {
        let mut conn = self.connection().await?;
        redis::cmd("CLIENT")
            .arg("ID")
            .query_async(&mut conn)
            .await
            .map_err(|err| backend_transport_status("redis", "client_id", err))
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
                let scoped = scope_redis_key(self.namespace(), key);
                let value: Option<Vec<u8>> = conn
                    .get(&scoped)
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
                let scoped_keys: Vec<String> = keys
                    .iter()
                    .map(|k| scope_redis_key(self.namespace(), k))
                    .collect();
                let values: Vec<Option<Vec<u8>>> = conn
                    .get(&scoped_keys)
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
                let scoped = scope_redis_key(self.namespace(), key);
                let exists: bool = conn
                    .exists(&scoped)
                    .await
                    .map_err(|err| backend_transport_status("redis", "EXISTS", err))?;
                Ok(json!({ "key": key, "exists": exists }).to_string())
            }
            "scan" | "cache_scan" => {
                let caller_pattern = spec.get("pattern").and_then(Json::as_str).unwrap_or("*");
                // C6: force the pattern under the tenant/project namespace so a raw
                // `scan` cannot enumerate keys + values across tenants.
                let pattern = effective_scan_pattern(self.namespace(), caller_pattern);
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
        // C6: scope writes under the tenant/project namespace too, so a scoped
        // read/scan agrees with what was written and no raw write can target
        // another tenant's key.
        let key = scope_redis_key(self.namespace(), key);
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

    // ── C6: raw scan/key ops are confined to the tenant/project namespace ─────

    #[test]
    fn redis_effective_scan_pattern_is_tenant_scoped() {
        let ns = redis_context_namespace("p1", "t1");
        assert_eq!(ns, "udb:p1:t1:");
        // A wildcard is anchored under the namespace.
        assert_eq!(effective_scan_pattern(Some(&ns), "*"), "udb:p1:t1:*");
        // A suffix pattern is anchored, not free.
        assert_eq!(
            effective_scan_pattern(Some(&ns), "orders:*"),
            "udb:p1:t1:orders:*"
        );
        // An in-namespace pattern is idempotent.
        assert_eq!(
            effective_scan_pattern(Some(&ns), "udb:p1:t1:orders:*"),
            "udb:p1:t1:orders:*"
        );
        // A cross-tenant pattern cannot escape our namespace — it is re-anchored
        // beneath it, so it matches nothing rather than another tenant's keys.
        assert_eq!(
            effective_scan_pattern(Some(&ns), "udb:p2:t2:*"),
            "udb:p1:t1:udb:p2:t2:*"
        );
        // Context-free path is unchanged.
        assert_eq!(effective_scan_pattern(None, "*"), "*");
    }

    #[test]
    fn redis_scope_key_confines_to_namespace() {
        let ns = redis_context_namespace("p1", "t1");
        assert_eq!(scope_redis_key(Some(&ns), "k"), "udb:p1:t1:k");
        // Already-scoped key is left as-is.
        assert_eq!(scope_redis_key(Some(&ns), "udb:p1:t1:k"), "udb:p1:t1:k");
        // A cross-tenant key is prefixed so it can't address another namespace.
        assert_eq!(
            scope_redis_key(Some(&ns), "udb:p2:t2:k"),
            "udb:p1:t1:udb:p2:t2:k"
        );
        assert_eq!(scope_redis_key(None, "k"), "k");
    }

    #[test]
    fn redis_context_namespace_defaults_empty_ids() {
        assert_eq!(redis_context_namespace("", ""), "udb:default:default:");
        assert_eq!(redis_context_namespace("  ", "t"), "udb:default:t:");
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
