//! Native `LiveQueryService` (master-plan 9.7) — query results that update
//! themselves. A client subscribes to a tenant-scoped query over a source proto
//! entity and receives an initial `Snapshot` (the rows currently matching an
//! IR-expressible filter) followed by an open server stream of `Change` deltas
//! (insert / update / delete) as the underlying data mutates.
//!
//! Tenant isolation is the entire point of 9.7, so it is enforced twice, both
//! fail closed:
//!
//! 1. **Snapshot** — produced ONLY through the mediated IR read path
//!    ([`DataBrokerRuntime::native_entity_read_for_service`]) with the tenant
//!    predicate injected server-side from the VERIFIED claim ([`predicate::snapshot_filter`]).
//! 2. **Delta stream** — every CDC event is re-checked, fail closed, against the
//!    subscriber's tenant scope ([`predicate::event_matches_tenant_scope`]) before
//!    the subscription's IR predicate is evaluated ([`predicate::filter_matches_row`]).
//!
//! Backpressure ([`stream::run_delta_forward`]): deltas are forwarded over a
//! BOUNDED `tokio::sync::mpsc` channel; on lag or a slow subscriber the stream is
//! CLOSED with `resource_exhausted` rather than buffering without bound. A
//! per-tenant CONCURRENT-stream budget ([`budget`]) caps open streams.
//!
//! Module layout: `config` (env knobs), `errors` (typed Status), `budget`
//! (per-tenant stream limiter), `predicate` (source resolution + IR filter +
//! single-row evaluator + CDC parsing), `stream` (delta forwarder), `handlers`
//! (the Subscribe RPC body). `mod.rs` holds only the service struct, the trait
//! delegator, and the builder.

use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::cdc::CdcEngine;
use crate::metrics::{MetricsRecorder, NoopMetrics};
use crate::proto::udb::core::livequery::services::v1 as lq_pb;
use crate::proto::udb::core::livequery::services::v1::live_query_service_server::LiveQueryService;
use crate::runtime::DataBrokerRuntime;
use crate::runtime::channels::ChannelManager;

pub use crate::proto::udb::core::livequery::services::v1::live_query_service_server::LiveQueryServiceServer;

use super::DataBrokerService;

mod budget;
mod config;
mod errors;
mod handlers;
mod predicate;
mod stream;
#[cfg(test)]
mod tests;

use errors::livequery_capability_status;
use stream::LiveQueryStream;

/// Postgres-backed `LiveQueryService` handler.
pub struct LiveQueryServiceImpl {
    /// Runtime handle for the mediated snapshot read path (IR query plan +
    /// read-fence + server-side tenant predicate). `None` fails closed.
    runtime: Option<Arc<DataBrokerRuntime>>,
    /// CDC engine the delta stream subscribes to. `None` means no live feed is
    /// available (slim deployments) — the snapshot is served and the stream ends.
    cdc_engine: Option<Arc<CdcEngine>>,
    /// Shared per-tenant fair-admission manager (rate-bounds new subscriptions).
    channels: Option<ChannelManager>,
    metrics: Arc<dyn MetricsRecorder>,
}

impl LiveQueryServiceImpl {
    pub fn new() -> Self {
        Self {
            runtime: None,
            cdc_engine: None,
            channels: None,
            metrics: Arc::new(NoopMetrics),
        }
    }

    pub(crate) fn with_runtime(mut self, runtime: Option<Arc<DataBrokerRuntime>>) -> Self {
        self.runtime = runtime;
        self
    }

    pub(crate) fn with_cdc_engine(mut self, cdc_engine: Option<Arc<CdcEngine>>) -> Self {
        self.cdc_engine = cdc_engine;
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

    /// Snapshot reads are durable-only: fail closed when no runtime dispatch is
    /// configured. Returns a cloned `Arc` so the read can outlive the borrow.
    fn require_runtime(&self) -> Result<Arc<DataBrokerRuntime>, Status> {
        self.runtime.clone().ok_or_else(|| {
            livequery_capability_status(
                "native_entity_dispatch",
                "runtime_native_entity_dispatch",
                "live query service requires runtime native-entity dispatch (no runtime configured)",
            )
        })
    }
}

impl Default for LiveQueryServiceImpl {
    fn default() -> Self {
        Self::new()
    }
}

#[tonic::async_trait]
impl LiveQueryService for LiveQueryServiceImpl {
    type SubscribeStream = LiveQueryStream;

    async fn subscribe(
        &self,
        request: Request<lq_pb::SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        handlers::subscribe(self, request).await
    }
}

impl DataBrokerService {
    /// Build the native `LiveQueryService`, wired to the runtime mediated-read
    /// dispatch (snapshots), the CDC engine broadcast feed (deltas), and the
    /// shared fair-admission channels.
    pub(crate) fn build_livequery_service(&self) -> LiveQueryServiceImpl {
        let runtime = self.runtime.load_full();
        let channels = Some(runtime.channels().clone());
        LiveQueryServiceImpl::new()
            .with_runtime(Some(runtime))
            .with_cdc_engine(self.cdc_engine.clone())
            .with_channels(channels)
            .with_metrics(self.metrics.clone())
    }
}
