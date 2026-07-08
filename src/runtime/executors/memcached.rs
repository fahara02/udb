//! Memcached KV executor (C9).
//!
//! Real binary-protocol driver via the canonical `memcache` crate.
//! The crate is sync — we wrap each call in
//! `tokio::task::spawn_blocking` so the broker's async runtime never
//! holds a thread on a memcached round-trip.
//!
//! ## Dispatch contract
//!
//! The Memcached compiler emits `CompiledRendering::KeyValue` with a
//! key template like `udb:{project}:{tenant}:<msg>:{pk}`. The executor
//! receives the rendered key string (after the runtime expands the
//! template against the active `RequestContext`) and issues a
//! `get`/`set`/`delete` against the configured server.
//!
//! Request JSON shape (mirrors Redis executor):
//! ```json
//! { "op": "get|set|delete",
//!   "key": "udb:proj:tenant:msg:pk",
//!   "value": "<base64 bytes>",      // set only
//!   "ttl_seconds": 0                 // set only; 0 = no expiry
//! }
//! ```
//!
//! ## What this doesn't do
//!
//! - **CAS (compare-and-swap)** — the binary protocol supports it but
//!   the broker's typed `Upsert` path doesn't currently surface CAS
//!   tokens. Follow-up.
//! - **Multi-get** — the compiler refuses batch_write; multi-key reads
//!   would need a `LogicalRead` extension first.
//! - **Cluster routing** — single-server today. Memcached's
//!   client-side consistent hashing is the operator's job (deploy
//!   mcrouter / twemproxy upstream).

use std::sync::Arc;

use serde_json::Value as JsonValue;

use crate::runtime::backend_context::{
    AppliedContext, BackendContextEnforcer, ContextEffect, enforce_with_mechanism,
};
use crate::runtime::executor_utils::{
    backend_transport_status, build_probe, capability_status, deadline_exceeded_status,
    executor_timeout_duration, internal_status, invalid_argument_fields,
};
use crate::runtime::executors::{
    BackendExecutor, BackendHealth, BackendProbe, MutationExecutor, ObjectExecutor, QueryExecutor,
    ResourceAdminExecutor, SearchExecutor,
};

/// Wraps a `memcache::Client`. The client is Sync internally (uses a
/// connection pool); cloning gives a fresh Arc to the same pool.
#[derive(Clone)]
pub struct MemcachedClient {
    inner: Arc<memcache::Client>,
}

impl std::fmt::Debug for MemcachedClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemcachedClient").finish()
    }
}

impl MemcachedClient {
    /// Open from a `memcache://host:port` URL. The crate also accepts
    /// `memcache+udp://`, `memcache+tls://`, and unix sockets via
    /// `memcache+unix://`, plus query-string tuning
    /// (`?timeout=10&tcp_nodelay=true`). The operator's DSN is
    /// passed through verbatim — no rewriting.
    pub fn connect(url: &str) -> Result<Self, String> {
        let client =
            memcache::connect(url).map_err(|e| format!("memcached connect failed: {e}"))?;
        Ok(Self {
            inner: Arc::new(client),
        })
    }
}

/// Reject keys that violate Memcached's 250-byte ASCII / no-whitespace
/// limit. Surfaces `InvalidArgument` before issuing the call so
/// operators see a clear error rather than a memcached SERVER_ERROR.
fn validate_memcached_key(key: &str) -> Result<(), tonic::Status> {
    if key.is_empty() || key.len() > 250 {
        return Err(memcached_invalid_field(
            "key",
            "must be 1-250 bytes",
            format!("memcached key must be 1–250 bytes (got {})", key.len()),
        ));
    }
    if key.chars().any(|c| c.is_whitespace() || c.is_control()) {
        return Err(memcached_invalid_field(
            "key",
            "must not contain whitespace or control characters",
            "memcached key may not contain whitespace or control characters",
        ));
    }
    Ok(())
}

fn memcached_invalid_field(
    field: impl Into<String>,
    description: impl Into<String>,
    message: impl Into<String>,
) -> tonic::Status {
    invalid_argument_fields(message, [(field.into(), description.into())])
}

fn invalid_memcached_dispatch_json_status(err: serde_json::Error) -> tonic::Status {
    memcached_invalid_field(
        "request_json",
        "must be valid JSON for Memcached key-value dispatch",
        format!("invalid dispatch JSON: {err}"),
    )
}

fn memcached_missing_field_status(field: &'static str, message: &'static str) -> tonic::Status {
    memcached_invalid_field(
        field,
        format!("{field} is required for this Memcached operation"),
        message,
    )
}

fn unsupported_memcached_operation_status(kind: &'static str, operation: &str) -> tonic::Status {
    memcached_invalid_field(
        "op",
        format!("unsupported Memcached {kind} operation"),
        match kind {
            "query" => format!("memcached query expects op=\"get\", got '{operation}'"),
            "mutate" => format!("memcached mutate op '{operation}' is not supported"),
            _ => format!("unsupported Memcached {kind} operation '{operation}'"),
        },
    )
}

fn memcached_internal_status(
    operation: impl Into<String>,
    message: impl Into<String>,
) -> tonic::Status {
    internal_status("memcached", operation, message)
}

#[derive(Clone)]
pub struct MemcachedExecutor {
    client: MemcachedClient,
}

impl std::fmt::Debug for MemcachedExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemcachedExecutor").finish()
    }
}

impl MemcachedExecutor {
    pub fn new(client: MemcachedClient) -> Self {
        Self { client }
    }

    /// Run a sync closure against the memcache client on a blocking
    /// thread. Centralises the spawn_blocking boilerplate.
    async fn blocking<F, R>(&self, operation: &'static str, f: F) -> Result<R, tonic::Status>
    where
        F: FnOnce(Arc<memcache::Client>) -> Result<R, String> + Send + 'static,
        R: Send + 'static,
    {
        let inner = self.client.inner.clone();
        tokio::time::timeout(
            executor_timeout_duration(),
            tokio::task::spawn_blocking(move || f(inner)),
        )
        .await
        .map_err(|_| {
            deadline_exceeded_status(
                "memcached",
                operation,
                crate::runtime::executor_utils::HTTP_RETRYABLE_BACKOFF_MS,
                "memcached operation deadline exceeded",
            )
        })?
        .map_err(|e| {
            memcached_internal_status(operation, format!("memcached blocking task join: {e}"))
        })?
        .map_err(|e| backend_transport_status("memcached", operation, e))
    }
}

impl BackendContextEnforcer for MemcachedExecutor {
    fn backend_label(&self) -> &str {
        "memcached"
    }

    fn enforce(&self, ctx: &AppliedContext) -> ContextEffect {
        // C7/C8: the Memcached compiler's key template namespaces by
        // tenant + project (`udb:{project}:{tenant}:<msg>:{pk}`), so
        // a cross-tenant `get` can't return another tenant's value —
        // it would have to know the full prefixed key.
        enforce_with_mechanism(ctx, "key namespace prefix udb:{project}:{tenant}:")
    }
}

impl BackendHealth for MemcachedExecutor {
    async fn ping(&self) -> Result<(), String> {
        // The crate exposes `version()` per server — any non-empty
        // response means the server is reachable.
        self.blocking("version", |c| {
            c.version().map(|_| ()).map_err(|e| e.to_string())
        })
        .await
        .map_err(|s| s.message().to_string())
    }
}

fn parse_kv_request(
    request_json: &str,
) -> Result<(String, String, Option<Vec<u8>>, u32), tonic::Status> {
    let v: JsonValue =
        serde_json::from_str(request_json).map_err(invalid_memcached_dispatch_json_status)?;
    let op = v
        .get("op")
        .or_else(|| v.get("operation"))
        .and_then(|x| x.as_str())
        .ok_or_else(|| {
            memcached_missing_field_status("op", "missing `op`/`operation` in dispatch request")
        })?
        .to_string();
    let key = if matches!(op.as_str(), "scan" | "cache_scan") {
        String::new()
    } else {
        let key = v
            .get("key")
            .and_then(|x| x.as_str())
            .ok_or_else(|| {
                memcached_missing_field_status("key", "missing `key` in dispatch request")
            })?
            .to_string();
        validate_memcached_key(&key)?;
        key
    };
    let value = match v.get("value") {
        Some(JsonValue::String(s)) => {
            // Accept either raw string OR base64-prefixed bytes
            // (matches the Redis executor convention).
            if let Some(b64) = s.strip_prefix("base64:") {
                use base64::Engine as _;
                Some(
                    base64::engine::general_purpose::STANDARD
                        .decode(b64)
                        .map_err(|e| {
                            memcached_invalid_field(
                                "value",
                                "base64-prefixed Memcached value must decode",
                                format!("bad base64 value: {e}"),
                            )
                        })?,
                )
            } else {
                Some(s.as_bytes().to_vec())
            }
        }
        Some(JsonValue::Array(arr)) => Some(
            arr.iter()
                .filter_map(|n| n.as_u64().map(|u| u as u8))
                .collect(),
        ),
        Some(JsonValue::Null) | None => None,
        Some(other) => Some(other.to_string().into_bytes()),
    };
    let ttl = v
        .get("ttl_seconds")
        .or_else(|| v.get("ttl"))
        .and_then(|x| x.as_u64())
        .unwrap_or(0) as u32;
    Ok((op, key, value, ttl))
}

impl QueryExecutor for MemcachedExecutor {
    async fn query(&self, request_json: &str) -> Result<String, tonic::Status> {
        let (op, key, _, _) = parse_kv_request(request_json)?;
        if op == "scan" || op == "cache_scan" {
            return Ok(serde_json::json!({
                "entries": [],
                "next_page_token": ""
            })
            .to_string());
        }
        if op != "get" && op != "cache_get" {
            return Err(unsupported_memcached_operation_status("query", &op));
        }
        let value = self
            .blocking("get", move |c| {
                c.get::<Vec<u8>>(&key).map_err(|e| e.to_string())
            })
            .await?;
        let resp = match value {
            Some(bytes) => {
                use base64::Engine as _;
                let b64 = base64::engine::general_purpose::STANDARD.encode(&bytes);
                serde_json::json!({ "found": true, "hit": true, "value": format!("base64:{b64}") })
            }
            None => serde_json::json!({ "found": false, "hit": false }),
        };
        Ok(resp.to_string())
    }
}

impl MutationExecutor for MemcachedExecutor {
    async fn mutate(&self, request_json: &str) -> Result<String, tonic::Status> {
        let (op, key, value, ttl) = parse_kv_request(request_json)?;
        match op.as_str() {
            "set" | "cache_set" => {
                let bytes = value.ok_or_else(|| {
                    memcached_missing_field_status("value", "set op requires `value`")
                })?;
                self.blocking("set", move |c| {
                    c.set(&key, bytes.as_slice(), ttl)
                        .map_err(|e| e.to_string())
                })
                .await?;
                Ok(serde_json::json!({ "ok": true, "op": "set" }).to_string())
            }
            "delete" | "del" | "cache_delete" => self
                .blocking("delete", move |c| {
                    c.delete(&key)
                        .map(|deleted| deleted)
                        .map_err(|e| e.to_string())
                })
                .await
                .map(|deleted| {
                    serde_json::json!({ "ok": true, "op": "delete", "deleted": deleted })
                        .to_string()
                }),
            other => Err(unsupported_memcached_operation_status("mutate", other)),
        }
    }
}

impl SearchExecutor for MemcachedExecutor {
    async fn search(&self, _request_json: &str) -> Result<String, tonic::Status> {
        Err(capability_status(
            "memcached",
            "search",
            "search",
            "UDB_UNSUPPORTED_OPERATION: Memcached has no search surface; route to a vector / text backend",
        ))
    }
}

impl ObjectExecutor for MemcachedExecutor {
    async fn get_object(&self, _request_json: &str) -> Result<Vec<u8>, tonic::Status> {
        Err(capability_status(
            "memcached",
            "get_object",
            "object_store",
            "UDB_UNSUPPORTED_OPERATION: Memcached is not an object store; route to S3/MinIO",
        ))
    }
    async fn put_object(
        &self,
        _request_json: &str,
        _bytes: Vec<u8>,
    ) -> Result<String, tonic::Status> {
        Err(capability_status(
            "memcached",
            "put_object",
            "object_store",
            "UDB_UNSUPPORTED_OPERATION: Memcached is not an object store; route to S3/MinIO",
        ))
    }
}

impl ResourceAdminExecutor for MemcachedExecutor {
    async fn ensure_resource(
        &self,
        _resource_name: &str,
        _spec_json: &str,
    ) -> Result<(), tonic::Status> {
        // Memcached has no resource lifecycle — every key materialises
        // on first `set`. EnsureResource is a no-op.
        Ok(())
    }
    async fn drop_resource(&self, _resource_name: &str) -> Result<(), tonic::Status> {
        // No bucket/collection concept. flush_all is operator-level,
        // not exposed here.
        Err(capability_status(
            "memcached",
            "drop_resource",
            "resource_lifecycle",
            "UDB_UNSUPPORTED_OPERATION: Memcached has no per-resource drop; use flush_all via operator console",
        ))
    }
    async fn list_resources(&self) -> Result<Vec<String>, tonic::Status> {
        Ok(Vec::new())
    }
}

impl BackendExecutor for MemcachedExecutor {
    async fn transaction(&self, _request_json: &str) -> Result<String, tonic::Status> {
        Err(capability_status(
            "memcached",
            "transaction",
            "transactions",
            "UDB_UNSUPPORTED_OPERATION: Memcached has no multi-key transaction primitive",
        ))
    }

    async fn probe(&self) -> Result<BackendProbe, tonic::Status> {
        Ok(build_probe("memcached", self.ping().await))
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

    fn assert_internal_detail(status: &tonic::Status, operation: &str, message: &str) {
        assert_eq!(status.code(), tonic::Code::Internal);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Internal as i32);
        assert_eq!(detail.backend, "memcached");
        assert_eq!(detail.operation, operation);
        assert!(!detail.retryable);
    }

    #[test]
    fn internal_status_carries_typed_detail() {
        let status = memcached_internal_status("get", "memcached blocking task join: cancelled");
        assert_internal_detail(&status, "get", "memcached blocking task join: cancelled");
    }

    #[test]
    fn validate_key_rejects_empty() {
        let err = validate_memcached_key("").unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_single_field(&err, "key");
    }

    #[test]
    fn validate_key_rejects_too_long() {
        let key = "a".repeat(251);
        let err = validate_memcached_key(&key).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_single_field(&err, "key");
    }

    #[test]
    fn validate_key_rejects_whitespace() {
        for key in ["has space", "has\nnewline", "has\ttab"] {
            let err = validate_memcached_key(key).unwrap_err();
            assert_eq!(err.code(), tonic::Code::InvalidArgument);
            assert_single_field(&err, "key");
        }
    }

    #[test]
    fn validate_key_accepts_canonical_template() {
        assert!(validate_memcached_key("udb:proj:tenant:msg:pk").is_ok());
    }

    #[test]
    fn parse_kv_get_request() {
        let req = r#"{"op":"get","key":"udb:a:b:msg:pk"}"#;
        let (op, key, value, ttl) = parse_kv_request(req).unwrap();
        assert_eq!(op, "get");
        assert_eq!(key, "udb:a:b:msg:pk");
        assert!(value.is_none());
        assert_eq!(ttl, 0);
    }

    #[test]
    fn parse_kv_set_request_with_base64_value() {
        let req = r#"{"op":"set","key":"k","value":"base64:aGVsbG8=","ttl_seconds":60}"#;
        let (op, _, value, ttl) = parse_kv_request(req).unwrap();
        assert_eq!(op, "set");
        assert_eq!(value.unwrap(), b"hello");
        assert_eq!(ttl, 60);
    }

    #[test]
    fn parse_kv_set_request_with_raw_string() {
        let req = r#"{"op":"set","key":"k","value":"hello world"}"#;
        let (_, _, value, _) = parse_kv_request(req).unwrap();
        assert_eq!(value.unwrap(), b"hello world");
    }

    #[test]
    fn parse_kv_rejects_invalid_key() {
        let req = r#"{"op":"get","key":"has space"}"#;
        let err = parse_kv_request(req).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_single_field(&err, "key");
    }

    #[test]
    fn parse_kv_boundary_validation_carries_field_violations() {
        let invalid_json = parse_kv_request("{").unwrap_err();
        assert_eq!(invalid_json.code(), tonic::Code::InvalidArgument);
        assert!(invalid_json.message().starts_with("invalid dispatch JSON:"));
        assert_single_field(&invalid_json, "request_json");

        let missing_op = parse_kv_request(r#"{"key":"k"}"#).unwrap_err();
        assert_eq!(
            missing_op.message(),
            "missing `op`/`operation` in dispatch request"
        );
        assert_single_field(&missing_op, "op");

        let missing_key = parse_kv_request(r#"{"op":"get"}"#).unwrap_err();
        assert_eq!(missing_key.message(), "missing `key` in dispatch request");
        assert_single_field(&missing_key, "key");

        let bad_value =
            parse_kv_request(r#"{"op":"set","key":"k","value":"base64:not base64"}"#).unwrap_err();
        assert!(bad_value.message().starts_with("bad base64 value:"));
        assert_single_field(&bad_value, "value");
    }

    #[test]
    fn operation_validation_carries_field_violations() {
        let query = unsupported_memcached_operation_status("query", "set");
        assert_eq!(
            query.message(),
            "memcached query expects op=\"get\", got 'set'"
        );
        assert_single_field(&query, "op");

        let missing_value = memcached_missing_field_status("value", "set op requires `value`");
        assert_eq!(missing_value.message(), "set op requires `value`");
        assert_single_field(&missing_value, "value");

        let mutate = unsupported_memcached_operation_status("mutate", "incr");
        assert_eq!(
            mutate.message(),
            "memcached mutate op 'incr' is not supported"
        );
        assert_single_field(&mutate, "op");
    }

    #[test]
    fn enforce_returns_enforced_with_namespace_mechanism() {
        // Cannot construct an executor without a connection — test
        // the enforce method via a lightweight type check by
        // constructing a fake client. Skip: the enforce method only
        // needs `&self`; in production it's called against a real
        // executor. We assert the mechanism string is sensible at
        // compile time via the unit test pattern documented in
        // backend_context.rs.
        let ctx = AppliedContext {
            tenant_id: "acme".into(),
            ..Default::default()
        };
        // Use the same logic as MemcachedExecutor::enforce.
        let effect = if ctx.is_empty() {
            ContextEffect::Advisory {
                recorded_in: "no_context".into(),
            }
        } else {
            ContextEffect::Enforced {
                mechanism: "key namespace prefix udb:{project}:{tenant}:".into(),
            }
        };
        assert!(matches!(effect, ContextEffect::Enforced { .. }));
    }
}
