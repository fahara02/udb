//! Native `WebhookService` (master-plan 9.4) — events delivered to the outside
//! world.
//!
//! Mirrors `tenant_service`/`scheduler_service`: proto-driven, no in-memory store,
//! no hand-mapped schema. A tenant registers an external HTTPS endpoint with a
//! topic subscription; the leader-elected delivery worker
//! ([`worker::run_webhook_delivery_once`]) consumes the tenant-BOUND CDC stream,
//! signs each event body with the per-endpoint secret (HMAC-SHA256 →
//! `X-Udb-Signature`), POSTs it with retries/backoff, dead-letters after
//! `max_attempts` (`udb.webhook.delivery.dead.v1`), and writes the durable delivery
//! journal.
//!
//! Security core (fail closed everywhere):
//!   - **SSRF guard** ([`security::validate_webhook_target_url`] /
//!     [`security::ip_is_blocked`]): every external target is https-only and must
//!     NOT resolve to a private/loopback/link-local/CGNAT/unspecified range.
//!     Enforced at endpoint WRITE (Create/Update) AND again at DELIVERY time
//!     ([`security::resolve_and_validate_target`]) to defeat DNS rebinding.
//!   - **Tenant-bound subscription** ([`security::webhook_event_matches_endpoint_scope`]):
//!     an event is delivered to an endpoint only when its payload tenant matches
//!     the endpoint's verified `tenant_id`. The subscription is NEVER pattern-only
//!     — a tenant-less or cross-tenant event is dropped (mirrors the CDC stream
//!     fail-closed rule `engine_tail::payload_value_matches_stream_scope`).
//!   - **Claim-scoped CRUD**: the body `tenant_id` must match the verified claim
//!     (`validate_request_tenant`); the signing secret is OUTPUT_VIEW_STORAGE_ONLY
//!     and is returned exactly once at creation, never surfaced by reads.
//!
//! The HMAC is built from the already-vendored `sha2` via
//! [`crate::runtime::security::hmac_sha256`] (the same manual HMAC the Vault/TOTP
//! lanes use) — no new crypto crate is introduced.
//!
//! Module layout (no god file): [`config`] the statics + attempt/backoff knobs,
//! [`errors`] the typed statuses + field/SSRF validators, [`security`] the SSRF
//! guard + HMAC signing + tenant-bound subscription matching, [`model`] the
//! manifest model + read projections + row decoders, [`events`] the best-effort
//! outbox emit, [`worker`] the leader-elected delivery worker + durable-journal
//! loaders, [`handlers`] the six RPCs — `mod.rs` keeps only the struct, the
//! builders/require-guards, and one-line trait delegators.
//!
//! NOTE: `build_webhook_service` + the constructor/wiring helpers are reachable
//! from `serve()`. When `http-client` is enabled, `serve()` also spawns
//! [`worker::run_webhook_delivery_worker_once`] under
//! `run_while_leader(singleton::WORKER_WEBHOOK_DELIVERY)`.
#![allow(dead_code)]

use std::sync::Arc;

use sqlx::PgPool;
use tonic::{Request, Response, Status};

use crate::metrics::{MetricsRecorder, NoopMetrics};
use crate::proto::udb::core::webhook::services::v1 as webhook_pb;
use crate::proto::udb::core::webhook::services::v1::webhook_service_server::WebhookService;
use crate::runtime::channels::ChannelManager;

pub use crate::proto::udb::core::webhook::services::v1::webhook_service_server::WebhookServiceServer;

use super::DataBrokerService;

mod config;
mod errors;
mod events;
mod handlers;
mod model;
mod security;
#[cfg(test)]
mod tests;
mod worker;

// Re-exported at the module root for the leader webhook-delivery worker (`serve()`)
// and the notification service's shared delivery-time SSRF guard.
pub(crate) use config::{
    WEBHOOK_DELIVERY_BATCH, webhook_delivery_interval, webhook_delivery_timeout,
};
pub(crate) use security::resolve_and_validate_target;
// The IdP federation fetch (OIDC discovery / JWKS / SAML metadata) reuses the
// SAME resolver rather than growing a second partial URL check.
pub(crate) use security::resolve_and_pin_target;
// `validate_webhook_target_url` is re-exported at the module root ONLY for the
// notification_service SSRF cross-check test; in-crate callers import it directly
// from `security`, so a non-test (`--bin`) build sees the re-export as unused.
// Gate it to test builds to keep that build warning-free without losing the test.
#[cfg(test)]
pub(crate) use security::validate_webhook_target_url;
#[cfg(feature = "http-client")]
pub(crate) use worker::run_webhook_delivery_worker_once;

/// Postgres-backed `WebhookService` handler.
pub struct WebhookServiceImpl {
    pub(crate) pg_pool: Option<PgPool>,
    /// Transactional-outbox relation; mutations enqueue events here.
    pub(crate) outbox_relation: Option<String>,
    /// Per-tenant fair-admission manager (same one the data plane uses). `None`
    /// only in bare unit-test construction (admit becomes a no-op).
    pub(crate) channels: Option<ChannelManager>,
    pub(crate) metrics: Arc<dyn MetricsRecorder>,
    /// NTF1: runtime handle used to seal/unseal the endpoint signing secret at
    /// rest. `None` only in bare unit-test construction (encryption skipped).
    pub(crate) runtime: Option<Arc<crate::runtime::DataBrokerRuntime>>,
}

impl WebhookServiceImpl {
    pub fn new() -> Self {
        Self {
            pg_pool: None,
            outbox_relation: None,
            channels: None,
            metrics: Arc::new(NoopMetrics),
            runtime: None,
        }
    }

    pub fn with_postgres(mut self, pool: Option<PgPool>) -> Self {
        self.pg_pool = pool;
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

    pub(crate) fn with_runtime(
        mut self,
        runtime: Option<Arc<crate::runtime::DataBrokerRuntime>>,
    ) -> Self {
        self.runtime = runtime;
        self
    }

    /// Webhook state is durable-only: fail closed when no Postgres pool exists.
    pub(crate) fn require_pool(&self) -> Result<&PgPool, Status> {
        self.pg_pool.as_ref().ok_or_else(|| {
            errors::webhook_capability_status(
                "postgres_store",
                "postgres_store",
                "webhook service requires a Postgres-backed store (no PG pool configured)",
            )
        })
    }
}

impl Default for WebhookServiceImpl {
    fn default() -> Self {
        Self::new()
    }
}

#[tonic::async_trait]
impl WebhookService for WebhookServiceImpl {
    async fn create_endpoint(
        &self,
        request: Request<webhook_pb::CreateEndpointRequest>,
    ) -> Result<Response<webhook_pb::CreateEndpointResponse>, Status> {
        handlers::create_endpoint(self, request).await
    }

    async fn get_endpoint(
        &self,
        request: Request<webhook_pb::GetEndpointRequest>,
    ) -> Result<Response<webhook_pb::GetEndpointResponse>, Status> {
        handlers::get_endpoint(self, request).await
    }

    async fn list_endpoints(
        &self,
        request: Request<webhook_pb::ListEndpointsRequest>,
    ) -> Result<Response<webhook_pb::ListEndpointsResponse>, Status> {
        handlers::list_endpoints(self, request).await
    }

    async fn update_endpoint(
        &self,
        request: Request<webhook_pb::UpdateEndpointRequest>,
    ) -> Result<Response<webhook_pb::UpdateEndpointResponse>, Status> {
        handlers::update_endpoint(self, request).await
    }

    async fn delete_endpoint(
        &self,
        request: Request<webhook_pb::DeleteEndpointRequest>,
    ) -> Result<Response<webhook_pb::DeleteEndpointResponse>, Status> {
        handlers::delete_endpoint(self, request).await
    }

    async fn list_deliveries(
        &self,
        request: Request<webhook_pb::ListDeliveriesRequest>,
    ) -> Result<Response<webhook_pb::ListDeliveriesResponse>, Status> {
        handlers::list_deliveries(self, request).await
    }
}

impl DataBrokerService {
    /// Build the native `WebhookService`, wired to the broker's Postgres pool, the
    /// shared outbox, and the per-tenant fair-admission manager.
    pub(crate) fn build_webhook_service(&self) -> WebhookServiceImpl {
        let runtime = self.runtime.load_full();
        // Native-service persistence resolves through the discovery seam: the
        // backend is read from this service's proto `native_service` binding, then a
        // health/weight-routed instance is chosen — not the process-global pool.
        let pg_pool = runtime
            .native_store_pool_for_service("webhook", true, "")
            .ok();
        let outbox = runtime.config().cdc.outbox_relation();
        let channels = Some(runtime.channels().clone());
        WebhookServiceImpl::new()
            .with_postgres(pg_pool)
            .with_outbox(Some(outbox))
            .with_channels(channels)
            .with_metrics(self.metrics.clone())
            .with_runtime(Some(runtime))
    }
}
