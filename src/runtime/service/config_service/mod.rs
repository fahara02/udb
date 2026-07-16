//! Native `ConfigService` (master-plan 9.8) — feature flags and runtime config.
//!
//! Mirrors `lock_service`/`tenant_service`: proto-driven, no in-memory store, no
//! hand-mapped schema. Flags are durable rows in the canonical store, scoped to
//! (tenant, project, environment) and isolated by RLS; the tenant is always taken
//! from the VERIFIED claim, never the request body.
//!
//! The heart of the service is a PURE evaluation core ([`eval::evaluate_flag`] +
//! [`eval::resolve_flag`]) with NO I/O: the identical algorithm ships in the SDK
//! so a client-side cache and the server compute byte-identical results. Scope
//! precedence is deterministic (environment > project > tenant-default) and
//! percentage rollout uses a STABLE FNV-1a hash of `flag_key + ":" + subject`, so
//! a given subject is sticky across nodes and releases. The `tests` fixtures are
//! the SDK<->server contract (they should ship via sdk-conformance).
//!
//! Doctrine (Phase 9): claim-derived tenant, durable state, a monotone per-row
//! `revision` bumped on every change, a `udb.config.flag.changed.v1` outbox event
//! per mutation, and a resolve-once (OnceLock) evaluation TTL — never a
//! per-request env read.
//!
//! Module layout (no god file): [`config`] statics + TTL, [`eval`] the pure
//! SDK-parity core, [`codec`] value encodings, [`store`] tenant-scoped IR access
//! + JSON decode, [`errors`] typed statuses + validation, [`events`] the change
//! outbox, [`handlers`] the five RPCs — `mod.rs` keeps only the struct, the
//! builder, and one-line trait delegators.

use std::sync::Arc;

use sqlx::PgPool;
use tonic::{Request, Response, Status};

use crate::metrics::{MetricsRecorder, NoopMetrics};
use crate::proto::udb::core::config::services::v1 as config_pb;
use crate::proto::udb::core::config::services::v1::config_service_server::ConfigService;
use crate::runtime::DataBrokerRuntime;
use crate::runtime::channels::ChannelManager;

pub use crate::proto::udb::core::config::services::v1::config_service_server::ConfigServiceServer;

use super::DataBrokerService;

mod codec;
mod config;
mod errors;
mod eval;
mod events;
mod handlers;
mod store;
#[cfg(test)]
mod tests;

/// Postgres-backed `ConfigService` handler.
pub struct ConfigServiceImpl {
    pub(crate) pg_pool: Option<PgPool>,
    pub(crate) runtime: Option<Arc<DataBrokerRuntime>>,
    pub(crate) outbox_relation: Option<String>,
    pub(crate) channels: Option<ChannelManager>,
    pub(crate) metrics: Arc<dyn MetricsRecorder>,
}

impl ConfigServiceImpl {
    pub fn new() -> Self {
        Self {
            pg_pool: None,
            runtime: None,
            outbox_relation: None,
            channels: None,
            metrics: Arc::new(NoopMetrics),
        }
    }

    pub fn with_postgres(mut self, pool: Option<PgPool>) -> Self {
        self.pg_pool = pool;
        self
    }

    pub(crate) fn with_runtime(mut self, runtime: Option<Arc<DataBrokerRuntime>>) -> Self {
        self.runtime = runtime;
        self
    }

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

    /// Flag state is durable-only: fail closed when no canonical/PG store exists.
    pub(crate) fn require_runtime(&self) -> Result<&DataBrokerRuntime, Status> {
        self.runtime.as_deref().ok_or_else(|| {
            errors::config_capability_status(
                "native_entity_dispatch",
                "runtime_native_entity_dispatch",
                "config service requires runtime native-entity dispatch (no runtime configured)",
            )
        })
    }
}

impl Default for ConfigServiceImpl {
    fn default() -> Self {
        Self::new()
    }
}

#[tonic::async_trait]
impl ConfigService for ConfigServiceImpl {
    async fn put_flag(
        &self,
        request: Request<config_pb::PutFlagRequest>,
    ) -> Result<Response<config_pb::PutFlagResponse>, Status> {
        handlers::put_flag(self, request).await
    }

    async fn get_flag(
        &self,
        request: Request<config_pb::GetFlagRequest>,
    ) -> Result<Response<config_pb::GetFlagResponse>, Status> {
        handlers::get_flag(self, request).await
    }

    async fn list_flags(
        &self,
        request: Request<config_pb::ListFlagsRequest>,
    ) -> Result<Response<config_pb::ListFlagsResponse>, Status> {
        handlers::list_flags(self, request).await
    }

    async fn delete_flag(
        &self,
        request: Request<config_pb::DeleteFlagRequest>,
    ) -> Result<Response<config_pb::DeleteFlagResponse>, Status> {
        handlers::delete_flag(self, request).await
    }

    async fn evaluate_flags(
        &self,
        request: Request<config_pb::EvaluateFlagsRequest>,
    ) -> Result<Response<config_pb::EvaluateFlagsResponse>, Status> {
        handlers::evaluate_flags(self, request).await
    }
}

impl DataBrokerService {
    /// Build the native `ConfigService`, wired to the broker's Postgres pool, the
    /// native-entity dispatch runtime, and the shared outbox. Served in `serve()`
    /// via `add_service(ConfigServiceServer::new(...))`.
    pub(crate) fn build_config_service(&self) -> ConfigServiceImpl {
        let runtime = self.runtime.load_full();
        let pg_pool = runtime
            .native_store_pool_for_service("config", true, "")
            .ok();
        let outbox = runtime.config().cdc.outbox_relation();
        let channels = Some(runtime.channels().clone());
        ConfigServiceImpl::new()
            .with_postgres(pg_pool)
            .with_runtime(Some(runtime))
            .with_outbox(Some(outbox))
            .with_channels(channels)
            .with_metrics(self.metrics.clone())
    }
}
