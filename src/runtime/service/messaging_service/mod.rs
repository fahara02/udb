//! Native `MessagingService`: the delivery service and key directory for
//! end-to-end encrypted messaging over MLS (RFC 9420).
//!
//! Clients do every cryptographic operation. The broker keeps a device key
//! directory, one-time KeyPackages, the PUBLIC state of each group (an OpenMLS
//! `PublicGroup`) and a per-device inbox of opaque MLS messages. It validates
//! each handshake commit against the public state, so the epoch is linear and
//! the roster is authoritative, and fans messages out without holding any group
//! secret or ever seeing plaintext. Group membership, sizes and timing are
//! visible to the broker; hiding that metadata is out of scope.
//!
//! Persistence is Postgres-only (the commit path needs a row lock and one
//! transaction across group, roster, inbox and outbox), resolved per request
//! through the service's native-store binding. Every inbox row commits with its
//! `udb.messaging.inbox.appended.v1` outbox event.
//!
//! Module layout: [`config`] constants and knobs, [`errors`] typed statuses,
//! [`mls`] the OpenMLS delivery-service validation (pure), [`model`] request
//! validation and DTO mapping, [`store`] the manifest-derived SQL, [`events`] the
//! outbox event, [`handlers`] the twelve RPCs, [`workers`] the leader-elected
//! inbox retention worker. `mod.rs` keeps the struct, builders and one-line trait
//! delegators.

use std::sync::Arc;

use sqlx::PgPool;
use tonic::metadata::MetadataMap;
use tonic::{Request, Response, Status};

use crate::metrics::{MetricsRecorder, NoopMetrics};
use crate::proto::udb::core::messaging::services::v1 as msg_pb;
use crate::proto::udb::core::messaging::services::v1::messaging_service_server::MessagingService;
use crate::runtime::DataBrokerRuntime;
use crate::runtime::channels::ChannelManager;

pub use crate::proto::udb::core::messaging::services::v1::messaging_service_server::MessagingServiceServer;

use super::DataBrokerService;
use super::native_helpers::project_scoped_native_service_context;

mod config;
mod errors;
mod events;
mod handlers;
mod mls;
mod model;
mod store;
#[cfg(test)]
mod tests;
mod workers;

// Re-exported for `serve()`, which spawns the retention worker under leader
// election.
pub(crate) use config::{INBOX_RETENTION_BATCH, inbox_retention_interval, inbox_ttl};
pub(crate) use workers::run_inbox_retention_all_projects;

/// The `StreamInbox` response stream.
pub(crate) type InboxStream = std::pin::Pin<
    Box<dyn futures::Stream<Item = Result<msg_pb::StreamInboxResponse, Status>> + Send + 'static>,
>;

/// Postgres-backed `MessagingService` handler.
pub struct MessagingServiceImpl {
    /// Runtime handle for the native-store binding (per-project Postgres pool).
    pub(crate) runtime: Option<Arc<DataBrokerRuntime>>,
    /// Configured outbox relation; `None` enqueues no inbox events.
    pub(crate) outbox_relation: Option<String>,
    /// Shared per-tenant fair-admission manager (same one the data plane uses).
    pub(crate) channels: Option<ChannelManager>,
    pub(crate) metrics: Arc<dyn MetricsRecorder>,
    /// Wakes following `StreamInbox` streams on this broker after an append
    /// commits. Appends made by another replica are picked up by the stream's
    /// poll interval instead.
    pub(crate) inbox_wake: Arc<tokio::sync::Notify>,
}

impl MessagingServiceImpl {
    pub fn new() -> Self {
        Self {
            runtime: None,
            outbox_relation: None,
            channels: None,
            metrics: Arc::new(NoopMetrics),
            inbox_wake: Arc::new(tokio::sync::Notify::new()),
        }
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

    /// Resolve the request's messaging store: the Postgres pool the service's
    /// native-store binding selects for the request's project. Fails closed
    /// when no runtime or no Postgres store is configured.
    pub(crate) fn store(
        &self,
        metadata: &MetadataMap,
        tenant_id: &str,
        write: bool,
    ) -> Result<(crate::RequestContext, PgPool), Status> {
        let runtime = self.runtime.as_deref().ok_or_else(|| {
            errors::messaging_capability_status(
                "native_store",
                "runtime_native_store",
                "messaging service requires a runtime with a Postgres native store (no runtime configured)",
            )
        })?;
        let mut context = project_scoped_native_service_context(metadata, tenant_id);
        let (pool, instance) =
            runtime.native_store_postgres_binding_for_service("messaging", write, &context)?;
        context.target_backend = "postgres".to_string();
        context.target_instance = instance.unwrap_or_default();
        Ok((context, pool))
    }
}

impl Default for MessagingServiceImpl {
    fn default() -> Self {
        Self::new()
    }
}

#[tonic::async_trait]
impl MessagingService for MessagingServiceImpl {
    type StreamInboxStream = InboxStream;

    async fn register_device(
        &self,
        request: Request<msg_pb::RegisterDeviceRequest>,
    ) -> Result<Response<msg_pb::RegisterDeviceResponse>, Status> {
        handlers::register_device(self, request).await
    }

    async fn list_devices(
        &self,
        request: Request<msg_pb::ListDevicesRequest>,
    ) -> Result<Response<msg_pb::ListDevicesResponse>, Status> {
        handlers::list_devices(self, request).await
    }

    async fn revoke_device(
        &self,
        request: Request<msg_pb::RevokeDeviceRequest>,
    ) -> Result<Response<msg_pb::RevokeDeviceResponse>, Status> {
        handlers::revoke_device(self, request).await
    }

    async fn publish_key_packages(
        &self,
        request: Request<msg_pb::PublishKeyPackagesRequest>,
    ) -> Result<Response<msg_pb::PublishKeyPackagesResponse>, Status> {
        handlers::publish_key_packages(self, request).await
    }

    async fn claim_key_packages(
        &self,
        request: Request<msg_pb::ClaimKeyPackagesRequest>,
    ) -> Result<Response<msg_pb::ClaimKeyPackagesResponse>, Status> {
        handlers::claim_key_packages(self, request).await
    }

    async fn get_key_package_stock(
        &self,
        request: Request<msg_pb::GetKeyPackageStockRequest>,
    ) -> Result<Response<msg_pb::GetKeyPackageStockResponse>, Status> {
        handlers::get_key_package_stock(self, request).await
    }

    async fn create_group(
        &self,
        request: Request<msg_pb::CreateGroupRequest>,
    ) -> Result<Response<msg_pb::CreateGroupResponse>, Status> {
        handlers::create_group(self, request).await
    }

    async fn submit_commit(
        &self,
        request: Request<msg_pb::SubmitCommitRequest>,
    ) -> Result<Response<msg_pb::SubmitCommitResponse>, Status> {
        handlers::submit_commit(self, request).await
    }

    async fn get_group_info(
        &self,
        request: Request<msg_pb::GetGroupInfoRequest>,
    ) -> Result<Response<msg_pb::GetGroupInfoResponse>, Status> {
        handlers::get_group_info(self, request).await
    }

    async fn send_message(
        &self,
        request: Request<msg_pb::SendMessageRequest>,
    ) -> Result<Response<msg_pb::SendMessageResponse>, Status> {
        handlers::send_message(self, request).await
    }

    async fn stream_inbox(
        &self,
        request: Request<msg_pb::StreamInboxRequest>,
    ) -> Result<Response<Self::StreamInboxStream>, Status> {
        handlers::stream_inbox(self, request).await
    }

    async fn ack_inbox(
        &self,
        request: Request<msg_pb::AckInboxRequest>,
    ) -> Result<Response<msg_pb::AckInboxResponse>, Status> {
        handlers::ack_inbox(self, request).await
    }
}

impl DataBrokerService {
    /// Build the native `MessagingService`, wired to the runtime (per-project
    /// Postgres store binding), the shared outbox, and fair admission.
    pub(crate) fn build_messaging_service(&self) -> MessagingServiceImpl {
        let runtime = self.runtime.load_full();
        let outbox = runtime.config().cdc.outbox_relation();
        let channels = Some(runtime.channels().clone());
        MessagingServiceImpl::new()
            .with_runtime(Some(runtime))
            .with_outbox(Some(outbox))
            .with_channels(channels)
            .with_metrics(self.metrics.clone())
    }
}
