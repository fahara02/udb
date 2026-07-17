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
use sqlx::PgPool;
use tonic::{Request, Response, Status};

use crate::metrics::{MetricsRecorder, NoopMetrics};
use crate::proto::udb::core::cache::services::v1 as cache_pb;
use crate::proto::udb::core::cache::services::v1::cache_service_server::CacheService;
use crate::runtime::channels::ChannelManager;

pub use crate::proto::udb::core::cache::services::v1::cache_service_server::CacheServiceServer;

use super::DataBrokerService;

mod config;
mod errors;
#[cfg(feature = "redis")]
mod events;
mod handlers;
mod keys;
#[cfg(feature = "redis")]
mod redis_engine;
#[cfg(test)]
mod tests;
mod worker;

// Re-exported at the module root for the leader cache-invalidation worker
// (`serve()`), which reads the resolve-once interval, the batch size, and the
// worker entrypoint by these names.
#[cfg(feature = "redis")]
pub(crate) use config::{CACHE_INVALIDATION_BATCH, cache_invalidation_interval};
#[cfg(feature = "redis")]
pub(crate) use worker::run_cache_invalidation_worker_once;

/// Redis-backed `CacheService` handler.
pub struct CacheServiceImpl {
    /// Cache backend. Acquired via `DataBrokerRuntime::redis_clone` (the canonical
    /// client primitive). Only present under the `redis` feature.
    #[cfg(feature = "redis")]
    pub(crate) redis: Option<redis::Client>,
    /// Outbox-event Postgres pool (best-effort event emission). `None` disables it.
    #[cfg(feature = "redis")]
    pub(crate) pg_pool: Option<PgPool>,
    /// Configured outbox relation; `None` disables event emission.
    #[cfg(feature = "redis")]
    pub(crate) outbox_relation: Option<String>,
    /// Shared per-tenant fair-admission manager (same one the data plane uses).
    pub(crate) channels: Option<ChannelManager>,
    pub(crate) metrics: Arc<dyn MetricsRecorder>,
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
    pub(crate) fn require_redis(&self) -> Result<&redis::Client, Status> {
        self.redis.as_ref().ok_or_else(|| {
            errors::redis_capability_status(
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
        handlers::get(self, request).await
    }

    async fn set(
        &self,
        request: Request<cache_pb::SetRequest>,
    ) -> Result<Response<cache_pb::SetResponse>, Status> {
        handlers::set(self, request).await
    }

    async fn delete(
        &self,
        request: Request<cache_pb::DeleteRequest>,
    ) -> Result<Response<cache_pb::DeleteResponse>, Status> {
        handlers::delete(self, request).await
    }

    async fn scan(
        &self,
        request: Request<cache_pb::ScanRequest>,
    ) -> Result<Response<cache_pb::ScanResponse>, Status> {
        handlers::scan(self, request).await
    }

    async fn create_namespace(
        &self,
        request: Request<cache_pb::CreateNamespaceRequest>,
    ) -> Result<Response<cache_pb::CreateNamespaceResponse>, Status> {
        handlers::create_namespace(self, request).await
    }

    async fn delete_namespace(
        &self,
        request: Request<cache_pb::DeleteNamespaceRequest>,
    ) -> Result<Response<cache_pb::DeleteNamespaceResponse>, Status> {
        handlers::delete_namespace(self, request).await
    }

    async fn get_namespace_stats(
        &self,
        request: Request<cache_pb::GetNamespaceStatsRequest>,
    ) -> Result<Response<cache_pb::GetNamespaceStatsResponse>, Status> {
        handlers::get_namespace_stats(self, request).await
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
