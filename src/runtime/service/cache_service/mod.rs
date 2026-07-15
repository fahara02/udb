//! Native `CacheService` (master-plan 9.6) — a cache that invalidates itself.
//!
//! Mirrors `tenant_service`/`lock_service`: proto-driven, no in-memory store, no
//! hand-mapped schema. This PROMOTES the four typed DataBroker cache RPCs
//! (`cache_get`/`cache_set`/`cache_delete`/`cache_scan`, which remain as additive
//! aliases on `DataBrokerService`) into a first-class native service that adds the
//! three things the generic typed RPCs do not: claim-scoped namespacing, a
//! per-tenant byte budget, and CDC-driven self-invalidation.
//!
//! Doctrine (Phase 9):
//!   - Every entry lives under `udb:cache:<tenant>:<ns>:k:<key>` where `<tenant>`
//!     comes from the VERIFIED bearer/claim tenant (`validate_request_tenant`),
//!     never a body-supplied value — so two tenants share a namespace safely and a
//!     caller can never read or sweep another tenant's keyspace.
//!   - Each namespace carries a per-tenant `max_bytes` budget tracked with a Redis
//!     `INCRBY` counter; a `Set` that would exceed it fails closed with
//!     `resource_exhausted` (the pure check is [`would_exceed_budget`]).
//!   - Prefix sweeps use Redis `SCAN` ([`SWEEP_COMMAND`]), NEVER `KEYS`, so a large
//!     keyspace never blocks the server.
//!   - Redis TTL expiry deletes entries WITHOUT running the `DECRBY` bookkeeping,
//!     so the `__bytes__` counter only ever drifts upward. The invalidation worker
//!     therefore runs a bounded byte-counter reconciliation pass each tick
//!     (rotating namespace subset, SCAN + STRLEN, never KEYS) that resets each
//!     visited counter to ground truth — see
//!     [`run_cache_invalidation_worker_once`].
//!   - The leader-elected CDC invalidation worker
//!     ([`run_cache_invalidation_once`], gated by `singleton::WORKER_CACHE_INVALIDATOR`)
//!     maps a source-table change to a namespace sweep and emits
//!     `udb.cache.invalidated.v1`. It derives the tenant ONLY from the event
//!     payload and skips tenant-less events, mirroring the CDC stream-scope
//!     fail-closed rule (`engine_tail::payload_value_matches_stream_scope`): a
//!     tenant-less event never triggers a cross-tenant sweep.
//!
//! Redis acquisition reuses the canonical-store client primitive
//! (`DataBrokerRuntime::redis_clone` → `get_multiplexed_async_connection`), the
//! same one `canonical_store::redis.rs` and `core/tx_object.rs` use — no second
//! connection layer is introduced here.

use std::sync::Arc;

#[cfg(feature = "redis")]
use sqlx::{PgPool, Row};
use tonic::{Request, Response, Status};

use crate::metrics::{MetricsRecorder, NoopMetrics};
use crate::proto::udb::core::cache::services::v1 as cache_pb;
use crate::proto::udb::core::cache::services::v1::cache_service_server::CacheService;
use crate::runtime::channels::{ChannelManager, OperationChannel};

pub use crate::proto::udb::core::cache::services::v1::cache_service_server::CacheServiceServer;

use super::DataBrokerService;
#[cfg(feature = "redis")]
use super::native_helpers::{
    NativeEventContext, enqueue_outbox_event_with_context, native_service_context,
};
use super::native_helpers::{admit_on as native_admit_on, validate_request_tenant};

/// Root prefix for every cache key. The full data-key shape is
/// `udb:cache:<tenant>:<ns>:k:<key>`; bookkeeping keys (the byte counter and the
/// namespace meta blob) share the `udb:cache:<tenant>:<ns>:` prefix but use a
/// reserved suffix so a data-key `SCAN` never returns them.
pub(crate) const KEY_ROOT: &str = "udb:cache";

/// The Redis command used for every prefix sweep. Load-bearing: the engine calls
/// `redis::cmd(SWEEP_COMMAND)`, and the guard test asserts it is `SCAN` (cursor,
/// non-blocking) and never `KEYS` (O(N), blocks the server).
pub(crate) const SWEEP_COMMAND: &str = "SCAN";

/// `COUNT` hint per SCAN round-trip (matches `core/tx_object::cache_delete_pattern`).
/// Also the `Scan` default page size when the caller sends no positive `limit`.
pub(crate) const SWEEP_COUNT: u32 = 500;

/// Hard cap on a caller-supplied `Scan` page `limit` (it becomes the SCAN `COUNT`
/// hint). Without the clamp a caller could push an arbitrarily large COUNT into
/// the cursor walk and turn SCAN into a KEYS-shaped stall — see
/// [`clamped_scan_count`], the pure gate `redis_engine::scan` applies.
pub(crate) const MAX_SCAN_PAGE_LIMIT: u32 = 1_000;

/// Byte-counter reconciliation bounds (the worker pass that heals TTL-expiry
/// drift — see `redis_engine::reconcile_bytes_counters_once`): at most this many
/// namespaces have their counter recomputed per worker pass (a rotating subset,
/// resumed via [`reconcile_cursor_key`]).
pub(crate) const RECONCILE_NAMESPACES_PER_PASS: usize = 16;

/// At most this many SCAN round-trips per pass while DISCOVERING namespace meta
/// keys, so discovery stays bounded even on a huge, meta-sparse keyspace.
pub(crate) const RECONCILE_DISCOVERY_MAX_ROUNDS: u32 = 32;

/// A namespace holding more data keys than this is SKIPPED for the pass instead
/// of being SET from a partial sum — a partial sum would UNDER-count usage and
/// quietly widen the byte budget (fail-open); skipping keeps the stale counter,
/// which only over-counts (fail-closed).
pub(crate) const RECONCILE_MAX_KEYS_PER_NAMESPACE: usize = 5_000;

/// Service-default per-namespace byte budget when a namespace declares none
/// (`max_bytes <= 0`). Bounds the shared Redis so one tenant/namespace cannot
/// exhaust it.
pub(crate) const DEFAULT_NAMESPACE_MAX_BYTES: i64 = 64 * 1024 * 1024;

/// Invalidation event topic emitted by `DeleteNamespace` and the CDC worker.
#[cfg(feature = "redis")]
const TOPIC_INVALIDATED: &str = "udb.cache.invalidated.v1";
#[cfg(feature = "redis")]
const TOPIC_ENTRY_SET: &str = "udb.cache.entry.set.v1";
#[cfg(feature = "redis")]
const TOPIC_ENTRY_DELETED: &str = "udb.cache.entry.deleted.v1";
#[cfg(feature = "redis")]
const TOPIC_NAMESPACE_CREATED: &str = "udb.cache.namespace.created.v1";
#[cfg(feature = "redis")]
pub(crate) const CACHE_INVALIDATION_BATCH: i64 = 200;
#[cfg(feature = "redis")]
const DEFAULT_CACHE_INVALIDATION_INTERVAL_SECS: u64 = 30;
#[cfg(feature = "redis")]
const CACHE_INVALIDATION_INTERVAL_ENV: &str = "UDB_CACHE_INVALIDATION_INTERVAL_SECS";

// ── pure key/namespacing helpers (always compiled; unit-tested without Redis) ──

/// The data-key for a namespace-local key. CLAIM-derived `tenant` is the first
/// path segment, so tenant-a and tenant-b never collide and a sweep of one
/// tenant's prefix can never reach another's.
pub(crate) fn data_key(tenant: &str, namespace: &str, key: &str) -> String {
    format!("{KEY_ROOT}:{tenant}:{namespace}:k:{key}")
}

/// The SCAN `MATCH` pattern for data keys under a namespace, optionally narrowed
/// by a namespace-local key prefix. Only `:k:` data keys match — the byte counter
/// and meta blob are excluded.
pub(crate) fn data_match(tenant: &str, namespace: &str, key_prefix: &str) -> String {
    format!("{KEY_ROOT}:{tenant}:{namespace}:k:{key_prefix}*")
}

/// The SCAN `MATCH` pattern for the ENTIRE namespace (data + counter + meta). Used
/// by the namespace flush and the CDC invalidation worker.
pub(crate) fn namespace_match_all(tenant: &str, namespace: &str) -> String {
    format!("{KEY_ROOT}:{tenant}:{namespace}:*")
}

/// The per-namespace used-bytes counter key (`INCRBY` target).
pub(crate) fn bytes_counter_key(tenant: &str, namespace: &str) -> String {
    format!("{KEY_ROOT}:{tenant}:{namespace}:__bytes__")
}

/// Reserved suffix of the per-namespace meta blob key (single definition so the
/// key builder, the reconciliation discovery pattern, and the parser never drift).
pub(crate) const META_SUFFIX: &str = "__meta__";

/// The per-namespace meta key (JSON: `max_bytes`, `default_ttl_seconds`).
pub(crate) fn meta_key(tenant: &str, namespace: &str) -> String {
    format!("{KEY_ROOT}:{tenant}:{namespace}:{META_SUFFIX}")
}

/// The SCAN `MATCH` pattern for EVERY namespace meta key across tenants. Used
/// ONLY by the leader-elected reconciliation pass, which re-derives the tenant
/// from each matched key itself — no caller-supplied value ever reaches this
/// pattern, so it cannot be steered cross-tenant.
pub(crate) fn meta_match_all() -> String {
    format!("{KEY_ROOT}:*:{META_SUFFIX}")
}

/// The bookkeeping key persisting the reconciliation rotation cursor across
/// worker passes. Lives directly under [`KEY_ROOT`] with a reserved segment and
/// NO namespace shape, so it never matches a data-key/meta SCAN pattern and no
/// tenant sweep can reach it.
pub(crate) fn reconcile_cursor_key() -> String {
    format!("{KEY_ROOT}:__reconcile__:cursor")
}

/// Recover `(tenant, namespace)` from a full meta key
/// (`udb:cache:<tenant>:<ns>:__meta__`). [`validate_namespace`] rejects `:` in a
/// namespace, so the namespace is always the LAST segment before the suffix and
/// everything in between belongs to the tenant. `None` for anything that is not
/// a well-formed meta key (data keys, counters, the rotation cursor).
pub(crate) fn parse_meta_key(full_key: &str) -> Option<(String, String)> {
    let rest = full_key.strip_prefix(&format!("{KEY_ROOT}:"))?;
    let rest = rest.strip_suffix(&format!(":{META_SUFFIX}"))?;
    let (tenant, namespace) = rest.rsplit_once(':')?;
    (!tenant.trim().is_empty() && !namespace.trim().is_empty())
        .then(|| (tenant.to_string(), namespace.to_string()))
}

/// Pure reconciliation math: the recomputed namespace usage is the saturating
/// sum of the (non-negative) `STRLEN` of every live data key the bounded SCAN
/// observed. Defensive: a bogus negative length never subtracts real usage, and
/// adversarially huge values saturate instead of wrapping.
pub(crate) fn reconciled_sum(lengths: &[i64]) -> i64 {
    lengths
        .iter()
        .fold(0i64, |acc, len| acc.saturating_add((*len).max(0)))
}

/// Clamp a caller-supplied `Scan` page limit into the SCAN `COUNT` hint: a
/// non-positive limit falls back to [`SWEEP_COUNT`]; a positive limit is capped
/// at [`MAX_SCAN_PAGE_LIMIT`] — never a raw pass-through.
pub(crate) fn clamped_scan_count(limit: i32) -> u32 {
    if limit > 0 {
        (limit as u32).min(MAX_SCAN_PAGE_LIMIT)
    } else {
        SWEEP_COUNT
    }
}

/// Recover the namespace-local key from a full data key (strip the
/// `udb:cache:<tenant>:<ns>:k:` prefix). `None` if the key is not a data key for
/// this (tenant, namespace).
pub(crate) fn strip_data_prefix<'a>(
    full_key: &'a str,
    tenant: &str,
    namespace: &str,
) -> Option<&'a str> {
    let prefix = format!("{KEY_ROOT}:{tenant}:{namespace}:k:");
    full_key.strip_prefix(&prefix)
}

/// Resolve a namespace's effective byte budget: a declared positive `max_bytes`
/// wins, otherwise the service default. Always returns a positive bound.
pub(crate) fn effective_max_bytes(declared: i64) -> i64 {
    if declared > 0 {
        declared
    } else {
        DEFAULT_NAMESPACE_MAX_BYTES
    }
}

/// The budget gate, pure and unit-tested without Redis: writing `delta` more bytes
/// to a namespace currently holding `used` bytes exceeds `max_bytes`. `delta <= 0`
/// (a shrink or same-size overwrite) never exceeds the budget.
pub(crate) fn would_exceed_budget(used: i64, delta: i64, max_bytes: i64) -> bool {
    delta > 0 && used.saturating_add(delta) > max_bytes
}

/// Reject an empty namespace, or one carrying the reserved key separator `:`
/// (which would let a caller escape its tenant/namespace prefix).
pub(crate) fn validate_namespace(namespace: &str) -> Result<String, Status> {
    let ns = namespace.trim();
    if ns.is_empty() {
        return Err(crate::runtime::executor_utils::invalid_argument_fields(
            "namespace is required",
            [("namespace", "must be a non-empty namespace")],
        ));
    }
    if ns.contains(':') || ns.contains(char::is_whitespace) {
        return Err(crate::runtime::executor_utils::invalid_argument_fields(
            "namespace must not contain ':' or whitespace",
            [("namespace", "must not contain ':' or whitespace")],
        ));
    }
    Ok(ns.to_string())
}

/// Reject an empty required string field.
pub(crate) fn require_field(name: &str, value: &str) -> Result<String, Status> {
    let v = value.trim();
    if v.is_empty() {
        return Err(crate::runtime::executor_utils::invalid_argument_fields(
            format!("{name} is required"),
            [(name, "must be a non-empty string")],
        ));
    }
    Ok(v.to_string())
}

#[cfg(not(feature = "redis"))]
fn no_redis_status() -> Status {
    redis_capability_status(
        "service_startup",
        "redis_feature",
        "cache service requires the `redis` feature/backend",
    )
}

fn redis_capability_status(
    operation: &'static str,
    capability_required: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::capability_status(
        "cache",
        operation,
        capability_required,
        message,
    )
}

/// Redis-backed `CacheService` handler.
pub struct CacheServiceImpl {
    /// Cache backend. Acquired via `DataBrokerRuntime::redis_clone` (the canonical
    /// client primitive). Only present under the `redis` feature.
    #[cfg(feature = "redis")]
    redis: Option<redis::Client>,
    /// Outbox-event Postgres pool (best-effort event emission). `None` disables it.
    #[cfg(feature = "redis")]
    pg_pool: Option<PgPool>,
    /// Configured outbox relation; `None` disables event emission.
    #[cfg(feature = "redis")]
    outbox_relation: Option<String>,
    /// Shared per-tenant fair-admission manager (same one the data plane uses).
    channels: Option<ChannelManager>,
    metrics: Arc<dyn MetricsRecorder>,
}

impl CacheServiceImpl {
    pub fn new() -> Self {
        Self {
            #[cfg(feature = "redis")]
            redis: None,
            #[cfg(feature = "redis")]
            pg_pool: None,
            #[cfg(feature = "redis")]
            outbox_relation: None,
            channels: None,
            metrics: Arc::new(NoopMetrics),
        }
    }

    #[cfg(feature = "redis")]
    pub(crate) fn with_redis(mut self, redis: Option<redis::Client>) -> Self {
        self.redis = redis;
        self
    }

    #[cfg(feature = "redis")]
    pub(crate) fn with_postgres(mut self, pool: Option<PgPool>) -> Self {
        self.pg_pool = pool;
        self
    }

    #[cfg(feature = "redis")]
    pub(crate) fn with_outbox(mut self, relation: Option<String>) -> Self {
        self.outbox_relation = relation;
        self
    }

    pub(crate) fn with_channels(mut self, channels: Option<ChannelManager>) -> Self {
        self.channels = channels;
        self
    }

    pub(crate) fn with_metrics(mut self, metrics: Arc<dyn MetricsRecorder>) -> Self {
        self.metrics = metrics;
        self
    }

    /// Cache state is Redis-only: fail closed when no backend is configured.
    #[cfg(feature = "redis")]
    fn require_redis(&self) -> Result<&redis::Client, Status> {
        self.redis.as_ref().ok_or_else(|| {
            redis_capability_status(
                "request_dispatch",
                "redis_backend",
                "cache service requires a configured Redis backend",
            )
        })
    }
}

impl Default for CacheServiceImpl {
    fn default() -> Self {
        Self::new()
    }
}

#[tonic::async_trait]
impl CacheService for CacheServiceImpl {
    async fn get(
        &self,
        request: Request<cache_pb::GetRequest>,
    ) -> Result<Response<cache_pb::GetResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        // Cross-tenant guard FIRST: the body tenant_id must match the verified
        // claim/header; after this passes it IS the verified tenant.
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let tenant = req.tenant_id.trim().to_string();
        let namespace = validate_namespace(&req.namespace)?;
        let key = require_field("key", &req.key)?;
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "cache",
            OperationChannel::Read,
            &tenant,
            None,
        )
        .await?;
        #[cfg(feature = "redis")]
        {
            let client = self.require_redis()?;
            let (found, value, ttl) = redis_engine::get(client, &tenant, &namespace, &key).await?;
            Ok(Response::new(cache_pb::GetResponse {
                found,
                value,
                ttl_remaining_seconds: ttl,
                error: None,
            }))
        }
        #[cfg(not(feature = "redis"))]
        {
            let _ = (&tenant, &namespace, &key);
            Err(no_redis_status())
        }
    }

    async fn set(
        &self,
        request: Request<cache_pb::SetRequest>,
    ) -> Result<Response<cache_pb::SetResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let tenant = req.tenant_id.trim().to_string();
        let namespace = validate_namespace(&req.namespace)?;
        let key = require_field("key", &req.key)?;
        // Set is an ordinary data mutation, so it shares the Write admission lane
        // with the sibling mutation RPCs (config/metering/notification writes and
        // the data-plane upsert handlers) — the Admin lane is reserved for
        // control-plane operations like namespace lifecycle.
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "cache",
            OperationChannel::Write,
            &tenant,
            None,
        )
        .await?;
        #[cfg(feature = "redis")]
        {
            let client = self.require_redis()?;
            let outcome = redis_engine::set(
                client,
                &tenant,
                &namespace,
                &key,
                &req.value,
                req.ttl_seconds,
            )
            .await?;
            self.emit_event(
                &metadata,
                TOPIC_ENTRY_SET,
                &tenant,
                &namespace,
                serde_json::json!({
                    "tenant_id": tenant,
                    "namespace": namespace,
                    "key": key,
                    "used_bytes": outcome.used_bytes,
                }),
            )
            .await;
            Ok(Response::new(cache_pb::SetResponse {
                stored: true,
                used_bytes: outcome.used_bytes,
                max_bytes: outcome.max_bytes,
                message: "cache entry stored".to_string(),
                error: None,
            }))
        }
        #[cfg(not(feature = "redis"))]
        {
            let _ = (&tenant, &namespace, &key, &req.value, req.ttl_seconds);
            Err(no_redis_status())
        }
    }

    async fn delete(
        &self,
        request: Request<cache_pb::DeleteRequest>,
    ) -> Result<Response<cache_pb::DeleteResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let tenant = req.tenant_id.trim().to_string();
        let namespace = validate_namespace(&req.namespace)?;
        let key = require_field("key", &req.key)?;
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "cache",
            OperationChannel::Admin,
            &tenant,
            None,
        )
        .await?;
        #[cfg(feature = "redis")]
        {
            let client = self.require_redis()?;
            let (deleted, used_bytes) =
                redis_engine::delete(client, &tenant, &namespace, &key).await?;
            if deleted {
                self.emit_event(
                    &metadata,
                    TOPIC_ENTRY_DELETED,
                    &tenant,
                    &namespace,
                    serde_json::json!({
                        "tenant_id": tenant,
                        "namespace": namespace,
                        "key": key,
                        "used_bytes": used_bytes,
                    }),
                )
                .await;
            }
            Ok(Response::new(cache_pb::DeleteResponse {
                deleted,
                used_bytes,
                message: if deleted {
                    "cache entry deleted".to_string()
                } else {
                    "cache entry not found".to_string()
                },
                error: None,
            }))
        }
        #[cfg(not(feature = "redis"))]
        {
            let _ = (&tenant, &namespace, &key);
            Err(no_redis_status())
        }
    }

    async fn scan(
        &self,
        request: Request<cache_pb::ScanRequest>,
    ) -> Result<Response<cache_pb::ScanResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let tenant = req.tenant_id.trim().to_string();
        let namespace = validate_namespace(&req.namespace)?;
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "cache",
            OperationChannel::Read,
            &tenant,
            None,
        )
        .await?;
        #[cfg(feature = "redis")]
        {
            let client = self.require_redis()?;
            let (items, next) = redis_engine::scan(
                client,
                &tenant,
                &namespace,
                req.key_prefix.trim(),
                req.limit,
                req.page_token.trim(),
            )
            .await?;
            Ok(Response::new(cache_pb::ScanResponse {
                items,
                next_page_token: next,
                error: None,
            }))
        }
        #[cfg(not(feature = "redis"))]
        {
            let _ = (
                &tenant,
                &namespace,
                &req.key_prefix,
                req.limit,
                &req.page_token,
            );
            Err(no_redis_status())
        }
    }

    async fn create_namespace(
        &self,
        request: Request<cache_pb::CreateNamespaceRequest>,
    ) -> Result<Response<cache_pb::CreateNamespaceResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let tenant = req.tenant_id.trim().to_string();
        let namespace = validate_namespace(&req.namespace)?;
        let max_bytes = effective_max_bytes(req.max_bytes);
        let default_ttl = req.default_ttl_seconds.max(0);
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "cache",
            OperationChannel::Admin,
            &tenant,
            None,
        )
        .await?;
        #[cfg(feature = "redis")]
        {
            let client = self.require_redis()?;
            redis_engine::create_namespace(client, &tenant, &namespace, max_bytes, default_ttl)
                .await?;
            self.emit_event(
                &metadata,
                TOPIC_NAMESPACE_CREATED,
                &tenant,
                &namespace,
                serde_json::json!({
                    "tenant_id": tenant,
                    "namespace": namespace,
                    "max_bytes": max_bytes,
                    "default_ttl_seconds": default_ttl,
                }),
            )
            .await;
            Ok(Response::new(cache_pb::CreateNamespaceResponse {
                namespace,
                max_bytes,
                default_ttl_seconds: default_ttl,
                message: "cache namespace ready".to_string(),
                error: None,
            }))
        }
        #[cfg(not(feature = "redis"))]
        {
            let _ = (&tenant, &namespace, max_bytes, default_ttl);
            Err(no_redis_status())
        }
    }

    async fn delete_namespace(
        &self,
        request: Request<cache_pb::DeleteNamespaceRequest>,
    ) -> Result<Response<cache_pb::DeleteNamespaceResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let tenant = req.tenant_id.trim().to_string();
        let namespace = validate_namespace(&req.namespace)?;
        // DESTRUCTIVE namespace flush: an empty confirmation token fails CLOSED.
        if req.confirmation_token.trim().is_empty() {
            return Err(crate::runtime::executor_utils::invalid_argument_fields(
                "DeleteNamespace flushes the whole namespace; confirmation_token is required",
                [(
                    "confirmation_token",
                    "must be present to flush a cache namespace",
                )],
            ));
        }
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "cache",
            OperationChannel::Admin,
            &tenant,
            None,
        )
        .await?;
        #[cfg(feature = "redis")]
        {
            let client = self.require_redis()?;
            let keys_deleted = redis_engine::flush_namespace(client, &tenant, &namespace).await?;
            self.emit_event(
                &metadata,
                TOPIC_INVALIDATED,
                &tenant,
                &namespace,
                serde_json::json!({
                    "tenant_id": tenant,
                    "namespace": namespace,
                    "keys_invalidated": keys_deleted,
                    "reason": "delete_namespace",
                }),
            )
            .await;
            Ok(Response::new(cache_pb::DeleteNamespaceResponse {
                namespace,
                keys_deleted,
                message: "cache namespace flushed".to_string(),
                error: None,
            }))
        }
        #[cfg(not(feature = "redis"))]
        {
            let _ = (&tenant, &namespace);
            Err(no_redis_status())
        }
    }

    async fn get_namespace_stats(
        &self,
        request: Request<cache_pb::GetNamespaceStatsRequest>,
    ) -> Result<Response<cache_pb::GetNamespaceStatsResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let tenant = req.tenant_id.trim().to_string();
        let namespace = validate_namespace(&req.namespace)?;
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "cache",
            OperationChannel::Read,
            &tenant,
            None,
        )
        .await?;
        #[cfg(feature = "redis")]
        {
            let client = self.require_redis()?;
            let stats = redis_engine::stats(client, &tenant, &namespace).await?;
            Ok(Response::new(cache_pb::GetNamespaceStatsResponse {
                namespace,
                used_bytes: stats.used_bytes,
                max_bytes: stats.max_bytes,
                item_count: stats.item_count,
                error: None,
            }))
        }
        #[cfg(not(feature = "redis"))]
        {
            let _ = (&tenant, &namespace);
            Err(no_redis_status())
        }
    }
}

#[cfg(feature = "redis")]
impl CacheServiceImpl {
    /// Best-effort versioned dot-topic outbox event (mirrors `lock_service`).
    async fn emit_event(
        &self,
        metadata: &tonic::metadata::MetadataMap,
        topic: &str,
        tenant_id: &str,
        namespace: &str,
        payload: serde_json::Value,
    ) {
        let Some(pool) = self.pg_pool.as_ref() else {
            return;
        };
        let context = native_service_context(metadata, tenant_id, "");
        enqueue_outbox_event_with_context(
            pool,
            self.outbox_relation.as_deref(),
            topic,
            namespace,
            tenant_id,
            &context.project_id,
            payload,
            NativeEventContext {
                target_resource: namespace.to_string(),
                ..NativeEventContext::default()
            },
            Some(&self.metrics),
        )
        .await;
    }
}

// ── CDC-driven self-invalidation worker (leader-elected) ──────────────────────

/// A single source-table change event the invalidation worker consumes. The
/// tenant is taken from the CDC payload, never from a request body.
#[derive(Debug, Clone)]
pub(crate) struct CacheInvalidationEvent {
    /// Source CDC event id. Empty for direct/test callers; worker-loaded events
    /// carry this and stamp it into the invalidation event for durable dedupe.
    pub event_id: String,
    /// The CDC topic the event arrived on (carried through for tracing).
    pub topic: String,
    /// The changed source table; maps to the cache namespace to invalidate.
    pub source_table: String,
    /// The full CDC payload — inspected for `tenant_id` (fail-closed scoping).
    pub payload: serde_json::Value,
}

/// Fail-closed tenant scoping for an invalidation event: return the trimmed
/// payload `tenant_id` only when non-empty. A tenant-less event yields `None` and
/// is SKIPPED — it never triggers a cross-tenant namespace sweep. This mirrors the
/// CDC stream-scope rule in `engine_tail::payload_value_matches_stream_scope`
/// (#8): a payload that cannot prove which tenant it belongs to is not delivered.
pub(crate) fn invalidation_tenant_scope(payload: &serde_json::Value) -> Option<String> {
    let tenant = payload
        .get("tenant_id")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .trim();
    (!tenant.is_empty()).then(|| tenant.to_string())
}

/// Map a CDC source table to the cache namespace it invalidates. Convention: the
/// namespace shares the table's name. `None` for a table that carries no name.
pub(crate) fn namespace_for_source_table(source_table: &str) -> Option<String> {
    let table = source_table.trim();
    (!table.is_empty()).then(|| table.to_string())
}

#[cfg(feature = "redis")]
pub(crate) fn cache_invalidation_interval() -> std::time::Duration {
    std::time::Duration::from_secs(
        std::env::var(CACHE_INVALIDATION_INTERVAL_ENV)
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(DEFAULT_CACHE_INVALIDATION_INTERVAL_SECS),
    )
}

fn invalidation_source_from_payload(payload: &serde_json::Value) -> String {
    for key in ["source_table", "table_name", "message_type"] {
        if let Some(value) = payload.get(key).and_then(serde_json::Value::as_str) {
            let value = value.trim();
            if !value.is_empty() {
                return value.to_string();
            }
        }
    }
    String::new()
}

#[cfg(feature = "redis")]
async fn load_cache_invalidation_events(
    pool: &PgPool,
    journal_relation: &str,
    outbox_relation: &str,
    batch: i64,
) -> Result<Vec<CacheInvalidationEvent>, String> {
    let limit = batch.max(1);
    let rows = sqlx::query(&format!(
        "SELECT j.event_id::TEXT AS event_id, j.topic AS topic, j.payload::TEXT AS payload_json \
         FROM {journal_relation} j \
         WHERE j.delivery_state IN ('published', 'acked') \
           AND j.topic <> $1 \
           AND COALESCE(j.payload->>'tenant_id', '') <> '' \
           AND ( \
               COALESCE(j.payload->>'source_table', '') <> '' \
               OR COALESCE(j.payload->>'table_name', '') <> '' \
               OR COALESCE(j.payload->>'message_type', '') <> '' \
           ) \
           AND NOT EXISTS ( \
               SELECT 1 FROM {outbox_relation} o \
               WHERE o.topic = $1 \
                 AND o.payload->>'source_event_id' = j.event_id::TEXT \
           ) \
           AND NOT EXISTS ( \
               SELECT 1 FROM {journal_relation} done \
               WHERE done.topic = $1 \
                 AND done.payload->>'source_event_id' = j.event_id::TEXT \
           ) \
         ORDER BY j.published_at ASC, j.event_id ASC \
         LIMIT $2"
    ))
    .bind(TOPIC_INVALIDATED)
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(|err| format!("load cache invalidation events failed: {err}"))?;

    let mut events = Vec::with_capacity(rows.len());
    for row in rows {
        let payload_json: String = row
            .try_get("payload_json")
            .map_err(|err| format!("decode cache invalidation payload failed: {err}"))?;
        let payload: serde_json::Value = serde_json::from_str(&payload_json)
            .map_err(|err| format!("decode cache invalidation payload JSON failed: {err}"))?;
        let source_table = invalidation_source_from_payload(&payload);
        if source_table.trim().is_empty() {
            continue;
        }
        events.push(CacheInvalidationEvent {
            event_id: row
                .try_get("event_id")
                .map_err(|err| format!("decode cache invalidation event id failed: {err}"))?,
            topic: row
                .try_get("topic")
                .map_err(|err| format!("decode cache invalidation topic failed: {err}"))?,
            source_table,
            payload,
        });
    }
    Ok(events)
}

/// Run ONE leader pass of CDC-driven cache invalidation (the consumer shape the
/// leader spawns under `run_while_leader(WORKER_CACHE_INVALIDATOR)`). For each
/// source-table change event it: derives the tenant fail-closed from the payload
/// (skipping tenant-less events), `SCAN`+`DEL`-sweeps the matching namespace for
/// that tenant, and emits `udb.cache.invalidated.v1`. Returns the total number of
/// cache keys invalidated. Best-effort per event: a single failing sweep is logged
/// and skipped rather than aborting the whole pass.
#[cfg_attr(test, allow(dead_code))]
#[allow(dead_code)]
#[cfg(feature = "redis")]
pub(crate) async fn run_cache_invalidation_once(
    redis: &redis::Client,
    outbox_pool: &PgPool,
    outbox_relation: Option<&str>,
    events: &[CacheInvalidationEvent],
    metrics: Option<&Arc<dyn MetricsRecorder>>,
) -> Result<u64, String> {
    let mut total: u64 = 0;
    for event in events {
        // Fail-closed: a tenant-less event never sweeps another tenant's keyspace.
        let Some(tenant) = invalidation_tenant_scope(&event.payload) else {
            tracing::debug!(
                topic = %event.topic,
                "cache invalidation: skipping tenant-less event"
            );
            continue;
        };
        let Some(namespace) = namespace_for_source_table(&event.source_table) else {
            continue;
        };
        let deleted = match redis_engine::flush_namespace(redis, &tenant, &namespace).await {
            Ok(deleted) => deleted,
            Err(err) => {
                tracing::warn!(
                    tenant = %tenant,
                    namespace = %namespace,
                    error = %err,
                    "cache invalidation sweep failed; skipping event"
                );
                continue;
            }
        };
        total = total.saturating_add(deleted);
        // Emit the invalidation event so downstream consumers (and live-query/
        // read-fence machinery) observe the namespace turn over.
        enqueue_outbox_event_with_context(
            outbox_pool,
            outbox_relation,
            TOPIC_INVALIDATED,
            &namespace,
            &tenant,
            "",
            serde_json::json!({
                "tenant_id": tenant,
                "namespace": namespace,
                "keys_invalidated": deleted,
                "source_event_id": event.event_id,
                "source_table": event.source_table,
                "reason": "cdc_source_change",
            }),
            NativeEventContext {
                operation: "cache.invalidate".to_string(),
                target_resource: namespace.clone(),
                ..NativeEventContext::default()
            },
            metrics,
        )
        .await;
    }
    Ok(total)
}

/// One full worker tick: CDC-driven namespace invalidation, then a bounded
/// byte-counter reconciliation pass.
///
/// Reconciliation exists because Redis TTL expiry deletes data keys WITHOUT
/// running the `DECRBY` bookkeeping that `delete` performs, so `__bytes__` only
/// ever drifts UPWARD until a namespace hits false `resource_exhausted`. Each
/// tick recomputes a rotating subset of namespaces from ground truth
/// (SCAN + STRLEN) and SETs the counter to the recomputed absolute sum — see
/// `redis_engine::reconcile_bytes_counters_once` for the bounds. Best-effort:
/// a failed reconciliation never aborts the invalidation result, because a
/// stale counter only over-counts, which fails CLOSED toward refusing writes.
#[cfg(feature = "redis")]
pub(crate) async fn run_cache_invalidation_worker_once(
    redis: &redis::Client,
    outbox_pool: &PgPool,
    outbox_relation: &str,
    journal_relation: &str,
    batch: i64,
    metrics: Option<&Arc<dyn MetricsRecorder>>,
) -> Result<i64, String> {
    let events =
        load_cache_invalidation_events(outbox_pool, journal_relation, outbox_relation, batch)
            .await?;
    let invalidated =
        run_cache_invalidation_once(redis, outbox_pool, Some(outbox_relation), &events, metrics)
            .await?;
    match redis_engine::reconcile_bytes_counters_once(redis).await {
        Ok(reconciled) if reconciled > 0 => {
            tracing::debug!(
                namespaces = reconciled,
                "cache byte-counter reconciliation pass completed"
            );
        }
        Ok(_) => {}
        Err(err) => {
            tracing::warn!(
                error = %err,
                "cache byte-counter reconciliation pass failed; counters stay \
                 eventually consistent until the next pass"
            );
        }
    }
    Ok(i64::try_from(invalidated).unwrap_or(i64::MAX))
}

// ── Redis engine (all `redis::` access lives here; gated on the feature) ──────

#[cfg(feature = "redis")]
mod redis_engine {
    use super::*;

    fn map_err(context: &str, err: redis::RedisError) -> Status {
        crate::runtime::executor_utils::backend_transport_status("redis", context, err)
    }

    async fn connect(client: &redis::Client) -> Result<redis::aio::MultiplexedConnection, Status> {
        client
            .get_multiplexed_async_connection()
            .await
            .map_err(|err| {
                crate::runtime::executor_utils::backend_transport_status("redis", "connection", err)
            })
    }

    /// The decoded namespace meta blob (budget + default TTL).
    #[derive(Default)]
    struct NamespaceMeta {
        max_bytes: i64,
        default_ttl_seconds: i64,
    }

    async fn load_meta(
        conn: &mut redis::aio::MultiplexedConnection,
        tenant: &str,
        namespace: &str,
    ) -> Result<NamespaceMeta, Status> {
        let raw: Option<String> = redis::cmd("GET")
            .arg(meta_key(tenant, namespace))
            .query_async(conn)
            .await
            .map_err(|err| map_err("GET meta", err))?;
        let Some(raw) = raw else {
            return Ok(NamespaceMeta::default());
        };
        let value: serde_json::Value = serde_json::from_str(&raw).unwrap_or_default();
        Ok(NamespaceMeta {
            max_bytes: value
                .get("max_bytes")
                .and_then(serde_json::Value::as_i64)
                .unwrap_or(0),
            default_ttl_seconds: value
                .get("default_ttl_seconds")
                .and_then(serde_json::Value::as_i64)
                .unwrap_or(0),
        })
    }

    async fn read_counter(
        conn: &mut redis::aio::MultiplexedConnection,
        tenant: &str,
        namespace: &str,
    ) -> Result<i64, Status> {
        let used: Option<i64> = redis::cmd("GET")
            .arg(bytes_counter_key(tenant, namespace))
            .query_async(conn)
            .await
            .map_err(|err| map_err("GET counter", err))?;
        Ok(used.unwrap_or(0).max(0))
    }

    pub(super) async fn get(
        client: &redis::Client,
        tenant: &str,
        namespace: &str,
        key: &str,
    ) -> Result<(bool, Vec<u8>, i64), Status> {
        let mut conn = connect(client).await?;
        let full = data_key(tenant, namespace, key);
        let value: Option<Vec<u8>> = redis::cmd("GET")
            .arg(&full)
            .query_async(&mut conn)
            .await
            .map_err(|err| map_err("GET", err))?;
        match value {
            Some(value) => {
                let ttl: i64 = redis::cmd("TTL")
                    .arg(&full)
                    .query_async(&mut conn)
                    .await
                    .map_err(|err| map_err("TTL", err))?;
                Ok((true, value, ttl))
            }
            None => Ok((false, Vec::new(), -2)),
        }
    }

    pub(super) struct SetOutcome {
        pub used_bytes: i64,
        pub max_bytes: i64,
    }

    pub(super) async fn set(
        client: &redis::Client,
        tenant: &str,
        namespace: &str,
        key: &str,
        value: &[u8],
        ttl_seconds: i64,
    ) -> Result<SetOutcome, Status> {
        let mut conn = connect(client).await?;
        let full = data_key(tenant, namespace, key);
        let meta = load_meta(&mut conn, tenant, namespace).await?;
        let max_bytes = effective_max_bytes(meta.max_bytes);

        // Replacing an existing key only charges the size DELTA against the budget.
        let old_len: i64 = redis::cmd("STRLEN")
            .arg(&full)
            .query_async(&mut conn)
            .await
            .map_err(|err| map_err("STRLEN", err))?;
        let new_len = value.len() as i64;
        let delta = new_len - old_len;
        let used = read_counter(&mut conn, tenant, namespace).await?;
        if would_exceed_budget(used, delta, max_bytes) {
            return Err(crate::runtime::executor_utils::quota_refusal_status(
                "cache",
                "namespace byte budget",
                format!(
                    "cache namespace '{namespace}' byte budget exhausted \
                 (used {used} + {delta} > max {max_bytes})"
                ),
            ));
        }

        // Resolve TTL: explicit request value wins; else the namespace default; else
        // no expiry.
        let ttl = if ttl_seconds > 0 {
            ttl_seconds
        } else {
            meta.default_ttl_seconds.max(0)
        };
        let mut set_cmd = redis::cmd("SET");
        set_cmd.arg(&full).arg(value);
        if ttl > 0 {
            set_cmd.arg("EX").arg(ttl);
        }
        set_cmd
            .query_async::<()>(&mut conn)
            .await
            .map_err(|err| map_err("SET", err))?;

        let used_after: i64 = if delta != 0 {
            redis::cmd("INCRBY")
                .arg(bytes_counter_key(tenant, namespace))
                .arg(delta)
                .query_async(&mut conn)
                .await
                .map_err(|err| map_err("INCRBY", err))?
        } else {
            used
        };
        Ok(SetOutcome {
            used_bytes: used_after.max(0),
            max_bytes,
        })
    }

    pub(super) async fn delete(
        client: &redis::Client,
        tenant: &str,
        namespace: &str,
        key: &str,
    ) -> Result<(bool, i64), Status> {
        let mut conn = connect(client).await?;
        let full = data_key(tenant, namespace, key);
        let old_len: i64 = redis::cmd("STRLEN")
            .arg(&full)
            .query_async(&mut conn)
            .await
            .map_err(|err| map_err("STRLEN", err))?;
        let removed: i64 = redis::cmd("DEL")
            .arg(&full)
            .query_async(&mut conn)
            .await
            .map_err(|err| map_err("DEL", err))?;
        if removed == 0 {
            let used = read_counter(&mut conn, tenant, namespace).await?;
            return Ok((false, used));
        }
        let used_after: i64 = redis::cmd("INCRBY")
            .arg(bytes_counter_key(tenant, namespace))
            .arg(-old_len)
            .query_async(&mut conn)
            .await
            .map_err(|err| map_err("INCRBY", err))?;
        Ok((true, used_after.max(0)))
    }

    pub(super) async fn scan(
        client: &redis::Client,
        tenant: &str,
        namespace: &str,
        key_prefix: &str,
        limit: i32,
        page_token: &str,
    ) -> Result<(Vec<cache_pb::CacheItem>, String), Status> {
        let mut conn = connect(client).await?;
        let pattern = data_match(tenant, namespace, key_prefix);
        let cursor: u64 = page_token.parse().unwrap_or(0);
        // Named clamp: the caller limit is capped at MAX_SCAN_PAGE_LIMIT, never
        // passed through raw as the SCAN COUNT hint.
        let count = clamped_scan_count(limit);
        // SCAN, never KEYS: cursor-paged so a large keyspace never blocks Redis.
        let (next, keys): (u64, Vec<String>) = redis::cmd(SWEEP_COMMAND)
            .arg(cursor)
            .arg("MATCH")
            .arg(&pattern)
            .arg("COUNT")
            .arg(count)
            .query_async(&mut conn)
            .await
            .map_err(|err| map_err("SCAN", err))?;

        let mut items = Vec::with_capacity(keys.len());
        for full in &keys {
            let Some(local) = strip_data_prefix(full, tenant, namespace) else {
                continue;
            };
            let value: Option<Vec<u8>> = redis::cmd("GET")
                .arg(full)
                .query_async(&mut conn)
                .await
                .map_err(|err| map_err("GET", err))?;
            let Some(value) = value else { continue };
            let ttl: i64 = redis::cmd("TTL")
                .arg(full)
                .query_async(&mut conn)
                .await
                .map_err(|err| map_err("TTL", err))?;
            items.push(cache_pb::CacheItem {
                key: local.to_string(),
                value,
                ttl_remaining_seconds: ttl,
            });
        }
        // Redis SCAN signals end-of-iteration with cursor 0; surface "" then.
        let next_token = if next == 0 {
            String::new()
        } else {
            next.to_string()
        };
        Ok((items, next_token))
    }

    pub(super) async fn create_namespace(
        client: &redis::Client,
        tenant: &str,
        namespace: &str,
        max_bytes: i64,
        default_ttl_seconds: i64,
    ) -> Result<(), Status> {
        let mut conn = connect(client).await?;
        let meta = serde_json::json!({
            "max_bytes": max_bytes,
            "default_ttl_seconds": default_ttl_seconds,
        });
        redis::cmd("SET")
            .arg(meta_key(tenant, namespace))
            .arg(meta.to_string())
            .query_async::<()>(&mut conn)
            .await
            .map_err(|err| map_err("SET meta", err))?;
        Ok(())
    }

    /// SCAN+DEL sweep of the ENTIRE namespace (data keys + counter + meta).
    pub(super) async fn flush_namespace(
        client: &redis::Client,
        tenant: &str,
        namespace: &str,
    ) -> Result<u64, Status> {
        let mut conn = connect(client).await?;
        let pattern = namespace_match_all(tenant, namespace);
        let mut cursor: u64 = 0;
        let mut deleted: u64 = 0;
        loop {
            // SCAN, never KEYS.
            let (next, keys): (u64, Vec<String>) = redis::cmd(SWEEP_COMMAND)
                .arg(cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(SWEEP_COUNT)
                .query_async(&mut conn)
                .await
                .map_err(|err| map_err("SCAN", err))?;
            // Count only data keys toward the reported invalidation total; the
            // counter/meta bookkeeping keys are swept but not "cache entries".
            let data_keys: Vec<&String> = keys
                .iter()
                .filter(|k| strip_data_prefix(k, tenant, namespace).is_some())
                .collect();
            if !keys.is_empty() {
                let removed: u64 = redis::cmd("DEL")
                    .arg(&keys)
                    .query_async(&mut conn)
                    .await
                    .map_err(|err| map_err("DEL", err))?;
                deleted = deleted.saturating_add(removed.min(data_keys.len() as u64));
            }
            if next == 0 {
                break;
            }
            cursor = next;
        }
        Ok(deleted)
    }

    pub(super) struct NamespaceStats {
        pub used_bytes: i64,
        pub max_bytes: i64,
        pub item_count: u64,
    }

    pub(super) async fn stats(
        client: &redis::Client,
        tenant: &str,
        namespace: &str,
    ) -> Result<NamespaceStats, Status> {
        let mut conn = connect(client).await?;
        let used_bytes = read_counter(&mut conn, tenant, namespace).await?;
        let meta = load_meta(&mut conn, tenant, namespace).await?;
        let pattern = data_match(tenant, namespace, "");
        let mut cursor: u64 = 0;
        let mut item_count: u64 = 0;
        loop {
            // SCAN, never KEYS.
            let (next, keys): (u64, Vec<String>) = redis::cmd(SWEEP_COMMAND)
                .arg(cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(SWEEP_COUNT)
                .query_async(&mut conn)
                .await
                .map_err(|err| map_err("SCAN", err))?;
            item_count = item_count.saturating_add(keys.len() as u64);
            if next == 0 {
                break;
            }
            cursor = next;
        }
        Ok(NamespaceStats {
            used_bytes,
            max_bytes: effective_max_bytes(meta.max_bytes),
            item_count,
        })
    }

    /// One bounded byte-counter reconciliation pass (leader-elected; called from
    /// [`run_cache_invalidation_worker_once`]). Returns how many namespaces had
    /// their counter recomputed.
    ///
    /// Design + bounds:
    ///   - Namespaces are DISCOVERED by SCANning meta keys ([`meta_match_all`],
    ///     SCAN never KEYS) from a rotation cursor persisted under
    ///     [`reconcile_cursor_key`], so every namespace is eventually visited
    ///     round-robin across passes. Per pass: at most
    ///     [`RECONCILE_DISCOVERY_MAX_ROUNDS`] discovery SCAN round-trips and
    ///     [`RECONCILE_NAMESPACES_PER_PASS`] namespaces recomputed, each capped
    ///     at [`RECONCILE_MAX_KEYS_PER_NAMESPACE`] data keys.
    ///   - A cursor persisted across passes is still a valid SCAN cursor (Redis
    ///     cursors are stateless reverse-binary-iteration positions); a
    ///     concurrent keyspace resize can at worst skip or duplicate a namespace
    ///     within ONE rotation — the next rotation covers it, and the pass is
    ///     idempotent (it SETs an absolute recomputed value), so duplicates are
    ///     harmless.
    ///   - Eventual-consistency window: a namespace's counter is exact only at
    ///     the moment it is reconciled. Between visits, TTL expiries inflate it
    ///     (over-count = fails CLOSED toward `resource_exhausted`), and a
    ///     Set/Delete racing the recompute can leave the written sum stale by
    ///     that in-flight delta until the namespace's next turn. Both converge
    ///     on re-run.
    pub(super) async fn reconcile_bytes_counters_once(
        client: &redis::Client,
    ) -> Result<u64, Status> {
        let mut conn = connect(client).await?;
        // Resume the rotation where the previous pass stopped (0 = keyspace start).
        let stored: Option<String> = redis::cmd("GET")
            .arg(reconcile_cursor_key())
            .query_async(&mut conn)
            .await
            .map_err(|err| map_err("GET reconcile cursor", err))?;
        let mut cursor: u64 = stored.and_then(|v| v.parse().ok()).unwrap_or(0);
        let pattern = meta_match_all();
        let mut namespaces: Vec<(String, String)> = Vec::new();
        for _ in 0..RECONCILE_DISCOVERY_MAX_ROUNDS {
            // SCAN, never KEYS.
            let (next, keys): (u64, Vec<String>) = redis::cmd(SWEEP_COMMAND)
                .arg(cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(SWEEP_COUNT)
                .query_async(&mut conn)
                .await
                .map_err(|err| map_err("SCAN meta", err))?;
            namespaces.extend(keys.iter().filter_map(|key| parse_meta_key(key)));
            cursor = next;
            if cursor == 0 || namespaces.len() >= RECONCILE_NAMESPACES_PER_PASS {
                break;
            }
        }
        // SCAN may surface the same key more than once within an iteration; the
        // recompute is idempotent, but dedupe anyway so the per-pass namespace
        // budget is spent on DISTINCT namespaces.
        namespaces.sort();
        namespaces.dedup();
        namespaces.truncate(RECONCILE_NAMESPACES_PER_PASS);
        // Persist the rotation position so the NEXT pass continues from here.
        redis::cmd("SET")
            .arg(reconcile_cursor_key())
            .arg(cursor.to_string())
            .query_async::<()>(&mut conn)
            .await
            .map_err(|err| map_err("SET reconcile cursor", err))?;

        let mut reconciled: u64 = 0;
        for (tenant, namespace) in &namespaces {
            if reconcile_namespace_bytes(&mut conn, tenant, namespace)
                .await?
                .is_some()
            {
                reconciled = reconciled.saturating_add(1);
            }
        }
        Ok(reconciled)
    }

    /// Recompute ONE namespace's `__bytes__` counter from ground truth: SCAN the
    /// namespace's data keys (only `:k:` keys — bookkeeping is excluded by the
    /// pattern), STRLEN each, and SET the counter to the saturating sum
    /// ([`reconciled_sum`]). Returns `Ok(None)` — counter left untouched — when
    /// the namespace exceeds [`RECONCILE_MAX_KEYS_PER_NAMESPACE`]: a partial sum
    /// would UNDER-count and fail open, whereas the stale counter only
    /// over-counts. A key expiring between SCAN and STRLEN reads as length 0,
    /// which is exactly its live usage.
    async fn reconcile_namespace_bytes(
        conn: &mut redis::aio::MultiplexedConnection,
        tenant: &str,
        namespace: &str,
    ) -> Result<Option<i64>, Status> {
        let pattern = data_match(tenant, namespace, "");
        let mut cursor: u64 = 0;
        let mut lengths: Vec<i64> = Vec::new();
        loop {
            // SCAN, never KEYS.
            let (next, keys): (u64, Vec<String>) = redis::cmd(SWEEP_COMMAND)
                .arg(cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(SWEEP_COUNT)
                .query_async(&mut *conn)
                .await
                .map_err(|err| map_err("SCAN reconcile", err))?;
            for full in &keys {
                if strip_data_prefix(full, tenant, namespace).is_none() {
                    continue;
                }
                if lengths.len() >= RECONCILE_MAX_KEYS_PER_NAMESPACE {
                    tracing::debug!(
                        tenant = %tenant,
                        namespace = %namespace,
                        cap = RECONCILE_MAX_KEYS_PER_NAMESPACE,
                        "cache byte-counter reconciliation: namespace over the \
                         per-pass key cap; skipping (stale counter fails closed)"
                    );
                    return Ok(None);
                }
                let len: i64 = redis::cmd("STRLEN")
                    .arg(full)
                    .query_async(&mut *conn)
                    .await
                    .map_err(|err| map_err("STRLEN reconcile", err))?;
                lengths.push(len);
            }
            if next == 0 {
                break;
            }
            cursor = next;
        }
        let sum = reconciled_sum(&lengths);
        let current = read_counter(conn, tenant, namespace).await?;
        if lengths.is_empty() && current == 0 {
            // Nothing stored and no drift recorded: leave no ghost counter key
            // behind (e.g. right after a namespace flush swept the counter).
            return Ok(Some(0));
        }
        if current != sum {
            redis::cmd("SET")
                .arg(bytes_counter_key(tenant, namespace))
                .arg(sum)
                .query_async::<()>(&mut *conn)
                .await
                .map_err(|err| map_err("SET reconcile counter", err))?;
        }
        Ok(Some(sum))
    }
}

#[cfg(test)]
mod cache_scope_tests {
    use super::*;
    use crate::proto::{ErrorDetail, ErrorKind};
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;
    use tonic::metadata::MetadataValue;

    fn decode_detail(status: &Status) -> ErrorDetail {
        let raw = status
            .metadata()
            .get_bin(ERROR_DETAIL_METADATA_KEY)
            .expect("error-detail trailer present")
            .to_bytes()
            .expect("trailer decodes to bytes");
        crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
    }

    /// Claim-derived key namespacing: the same logical namespace+key for two
    /// tenants produces DISTINCT Redis keys, so tenant-a can never read tenant-b's
    /// entry. The tenant is always the first path segment.
    #[test]
    fn keys_are_isolated_per_tenant() {
        let a = data_key("tenant-a", "sessions", "u1");
        let b = data_key("tenant-b", "sessions", "u1");
        assert_ne!(a, b);
        assert_eq!(a, "udb:cache:tenant-a:sessions:k:u1");
        assert!(a.starts_with("udb:cache:tenant-a:"));
        assert!(b.starts_with("udb:cache:tenant-b:"));
        // A tenant's sweep pattern can never match another tenant's keys.
        let sweep_a = namespace_match_all("tenant-a", "sessions");
        assert!(!b.starts_with(sweep_a.trim_end_matches('*')));
    }

    #[test]
    fn cache_validation_statuses_carry_field_violations() {
        let missing_namespace =
            validate_namespace("  ").expect_err("empty namespace must be rejected");
        assert_eq!(missing_namespace.code(), tonic::Code::InvalidArgument);
        assert_eq!(missing_namespace.message(), "namespace is required");
        let detail = decode_detail(&missing_namespace);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, "namespace");
        assert_eq!(
            detail.field_violations[0].description,
            "must be a non-empty namespace"
        );

        let invalid_namespace =
            validate_namespace("bad:namespace").expect_err("reserved separator rejected");
        let detail = decode_detail(&invalid_namespace);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations[0].field, "namespace");
        assert_eq!(
            detail.field_violations[0].description,
            "must not contain ':' or whitespace"
        );

        let missing_key = require_field("key", "  ").expect_err("empty key must be rejected");
        assert_eq!(missing_key.code(), tonic::Code::InvalidArgument);
        assert_eq!(missing_key.message(), "key is required");
        let detail = decode_detail(&missing_key);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, "key");
        assert_eq!(
            detail.field_violations[0].description,
            "must be a non-empty string"
        );
    }

    #[tokio::test]
    async fn delete_namespace_missing_confirmation_token_carries_field_violation() {
        let svc = CacheServiceImpl::new();
        let mut request = Request::new(cache_pb::DeleteNamespaceRequest {
            tenant_id: "tenant-a".to_string(),
            namespace: "sessions".to_string(),
            confirmation_token: " ".to_string(),
            ..Default::default()
        });
        request
            .metadata_mut()
            .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
        let err = svc
            .delete_namespace(request)
            .await
            .expect_err("missing confirmation token must fail before backend access");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            err.message(),
            "DeleteNamespace flushes the whole namespace; confirmation_token is required"
        );
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, "confirmation_token");
        assert_eq!(
            detail.field_violations[0].description,
            "must be present to flush a cache namespace"
        );
    }

    #[test]
    fn cache_missing_redis_capability_carries_typed_detail() {
        let status = redis_capability_status(
            "request_dispatch",
            "redis_backend",
            "cache service requires a configured Redis backend",
        );
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert_eq!(
            status.message(),
            "cache service requires a configured Redis backend"
        );
        let detail = decode_detail(&status);
        assert_eq!(detail.kind, ErrorKind::Capability as i32);
        assert_eq!(detail.backend, "cache");
        assert_eq!(detail.operation, "request_dispatch");
        assert_eq!(detail.capability_required, "redis_backend");
        assert!(!detail.retryable);
    }

    /// `Set` over budget → resource_exhausted (the pure budget gate). A shrink or
    /// same-size overwrite (`delta <= 0`) is always allowed.
    #[test]
    fn over_budget_is_rejected() {
        // 900 used, +200 delta, max 1000 → 1100 > 1000 → exceeds.
        assert!(would_exceed_budget(900, 200, 1000));
        // Exactly at the budget is allowed.
        assert!(!would_exceed_budget(800, 200, 1000));
        // A same-size or shrinking overwrite is always allowed, even at budget.
        assert!(!would_exceed_budget(1000, 0, 1000));
        assert!(!would_exceed_budget(1000, -10, 1000));
        // Unconfigured namespace falls back to the positive default bound.
        assert_eq!(effective_max_bytes(0), DEFAULT_NAMESPACE_MAX_BYTES);
        assert_eq!(effective_max_bytes(2048), 2048);
    }

    /// Prefix sweeps MUST use SCAN, never KEYS, and the data-key match pattern is
    /// namespace-prefixed + wildcard-terminated.
    #[test]
    fn sweep_uses_scan_not_keys() {
        assert_eq!(SWEEP_COMMAND, "SCAN");
        assert_ne!(SWEEP_COMMAND, "KEYS");
        let pattern = data_match("tenant-a", "sessions", "u");
        assert_eq!(pattern, "udb:cache:tenant-a:sessions:k:u*");
        assert!(pattern.ends_with('*'));
    }

    /// Byte-counter reconciliation math: TTL expiry never runs the DECRBY
    /// bookkeeping, so the worker recomputes usage from ground truth — the
    /// counter is SET to the saturating sum of observed live data-key lengths,
    /// and the stale counter value plays NO part in the math.
    #[test]
    fn reconciliation_sum_math() {
        assert_eq!(reconciled_sum(&[]), 0);
        assert_eq!(reconciled_sum(&[128, 64, 8]), 200);
        // Expired-between-SCAN-and-STRLEN keys read as length 0: a no-op term.
        assert_eq!(reconciled_sum(&[100, 0, 50]), 150);
        // Defensive: a bogus negative length never subtracts real usage.
        assert_eq!(reconciled_sum(&[-16, 32]), 32);
        // Adversarially huge values saturate instead of wrapping.
        assert_eq!(reconciled_sum(&[i64::MAX, 1]), i64::MAX);
    }

    /// Reconciliation namespace discovery parses `(tenant, namespace)` back out
    /// of the meta key. The namespace (colon-free by validation) is always the
    /// LAST segment; data keys, counters, and the rotation cursor never parse as
    /// namespaces, so the pass can never recompute (or create) bookkeeping for a
    /// non-namespace key.
    #[test]
    fn reconciliation_meta_key_round_trip() {
        assert_eq!(
            parse_meta_key(&meta_key("tenant-a", "sessions")),
            Some(("tenant-a".to_string(), "sessions".to_string()))
        );
        // A ':' inside the tenant segment stays with the tenant.
        assert_eq!(
            parse_meta_key("udb:cache:ten:ant:sessions:__meta__"),
            Some(("ten:ant".to_string(), "sessions".to_string()))
        );
        assert_eq!(parse_meta_key(&data_key("t", "ns", "k1")), None);
        assert_eq!(parse_meta_key(&bytes_counter_key("t", "ns")), None);
        assert_eq!(parse_meta_key(&reconcile_cursor_key()), None);
        assert_eq!(parse_meta_key("udb:cache::__meta__"), None);
        assert_eq!(parse_meta_key("udb:cache:only-one-segment:__meta__"), None);
        // The discovery pattern targets ONLY meta keys.
        assert_eq!(meta_match_all(), "udb:cache:*:__meta__");
        assert!(meta_match_all().ends_with(META_SUFFIX));
    }

    /// The `Scan` page limit is clamped to the named cap: a caller can never
    /// push an unbounded COUNT hint into the SCAN cursor walk, and a
    /// non-positive limit falls back to the default page size.
    #[test]
    fn scan_limit_is_clamped() {
        assert_eq!(clamped_scan_count(0), SWEEP_COUNT);
        assert_eq!(clamped_scan_count(-1), SWEEP_COUNT);
        assert_eq!(clamped_scan_count(10), 10);
        assert_eq!(
            clamped_scan_count(MAX_SCAN_PAGE_LIMIT as i32),
            MAX_SCAN_PAGE_LIMIT
        );
        assert_eq!(clamped_scan_count(i32::MAX), MAX_SCAN_PAGE_LIMIT);
        assert!(SWEEP_COUNT <= MAX_SCAN_PAGE_LIMIT);
    }

    /// CDC invalidation is tenant-scoped fail-closed: a tenant-less event yields no
    /// scope (it is skipped, never sweeping a cross-tenant namespace); an event
    /// stamped with a tenant yields exactly that tenant.
    #[test]
    fn invalidation_scope_fails_closed() {
        assert_eq!(invalidation_tenant_scope(&serde_json::json!({})), None);
        assert_eq!(
            invalidation_tenant_scope(&serde_json::json!({"tenant_id": "  "})),
            None
        );
        assert_eq!(
            invalidation_tenant_scope(&serde_json::json!({"tenant_id": "tenant-a"})),
            Some("tenant-a".to_string())
        );
        assert_eq!(
            namespace_for_source_table("invoices"),
            Some("invoices".to_string())
        );
        assert_eq!(namespace_for_source_table("  "), None);
    }

    #[test]
    fn invalidation_source_uses_source_table_then_table_then_message_type() {
        assert_eq!(
            invalidation_source_from_payload(
                &serde_json::json!({"source_table": "invoices", "message_type": "ignored"})
            ),
            "invoices"
        );
        assert_eq!(
            invalidation_source_from_payload(&serde_json::json!({"table_name": "orders"})),
            "orders"
        );
        assert_eq!(
            invalidation_source_from_payload(
                &serde_json::json!({"message_type": "billing.Invoice"})
            ),
            "billing.Invoice"
        );
        assert_eq!(invalidation_source_from_payload(&serde_json::json!({})), "");
    }

    /// A caller scoped to tenant-a must not target tenant-b's namespace by putting
    /// a foreign tenant_id in the request BODY; the scope guard rejects this before
    /// any Redis access — mirrors `tenant_service`/`lock_service`.
    #[tokio::test]
    async fn get_rejects_cross_tenant_body() {
        let svc = CacheServiceImpl::new(); // no redis, no channels (admit no-op)
        let mut request = Request::new(cache_pb::GetRequest {
            tenant_id: "tenant-b".to_string(),
            namespace: "sessions".to_string(),
            key: "u1".to_string(),
        });
        request
            .metadata_mut()
            .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
        let err = svc
            .get(request)
            .await
            .expect_err("cross-tenant body must be rejected");
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }
}

impl DataBrokerService {
    /// Build the native `CacheService`, wired to the broker's Redis backend (the
    /// canonical-store client primitive), the configured native Postgres store for
    /// outbox events, and the shared fair-admission manager.
    pub(crate) fn build_cache_service(&self) -> CacheServiceImpl {
        let runtime = self.runtime.load_full();
        let channels = Some(runtime.channels().clone());
        let service = CacheServiceImpl::new()
            .with_channels(channels)
            .with_metrics(self.metrics.clone());
        #[cfg(feature = "redis")]
        let service = {
            // Native-service persistence resolves through the discovery seam: the
            // outbox PG store is read from this service's binding (best-effort), and
            // the Redis cache backend is the canonical client clone.
            let pg_pool = runtime
                .native_store_pool_for_service("cache", true, "")
                .ok();
            let outbox = runtime.config().cdc.outbox_relation();
            service
                .with_redis(runtime.redis_clone())
                .with_postgres(pg_pool)
                .with_outbox(Some(outbox))
        };
        service
    }
}
