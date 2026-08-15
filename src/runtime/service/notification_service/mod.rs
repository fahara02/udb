//! Native `NotificationService` — proto-driven Postgres CRUD over the UDB-owned
//! `udb_notification.{notification_logs,notification_templates,notification_preferences}`
//! tables. Like `auth_service`/`tenant_service`: no in-memory store, identifiers
//! resolved from the embedded proto manifest via [`NativeModel`].
//!
//! This is the control-plane surface (persist + query notification state,
//! templates, and preferences, and aggregate delivery stats). Actual outbound
//! delivery (SES/Twilio/FCM/webhook) is performed by separate delivery adapters;
//! `SendNotification` records the intent as a `NotificationLog` row.
//!
//! Module layout (no god file): [`config`] the entity message ids + delivery
//! knobs + stable error reasons + test gate + topic derivations, [`errors`] the
//! typed statuses + `error-reason` attachment + field validator, [`model`] the
//! enum<->db converters + models + projections + JSON/`PgRow` decoders + the
//! `{{placeholder}}` renderer, [`store`] the neutral-IR query/record builders +
//! opt-out lookup + shared delivery-attempt upsert, [`events`] the strict
//! transactional outbox operations, [`delivery`] the leader-elected delivery worker + generic provider
//! adapter (http-client-gated), [`handlers`] the twelve RPCs — `mod.rs` keeps only
//! the struct, the builders/require-guards, and one-line trait delegators.
//!
//! NOTE: `build_notification_service` + the constructor/wiring helpers are
//! reachable from `serve()`. When `http-client` is enabled, `serve()` also spawns
//! [`delivery::run_notification_delivery_worker_once`] under
//! `NativeWorkerHost::spawn_while_leader(singleton::WORKER_NOTIFICATION_DELIVERY)`.

use std::sync::Arc;

use sqlx::PgPool;
use tonic::{Request, Response, Status};

use crate::metrics::{MetricsRecorder, NoopMetrics};
use crate::proto::udb::core::notification::services::v1 as notif_pb;
use crate::proto::udb::core::notification::services::v1::notification_service_server::NotificationService;
use crate::runtime::DataBrokerRuntime;
use crate::runtime::catalog::{CatalogManager, DEFAULT_PROJECT_ID};
use crate::runtime::channels::ChannelManager;

pub use crate::proto::udb::core::notification::services::v1::notification_service_server::NotificationServiceServer;

use super::DataBrokerService;

mod config;
mod delivery;
mod errors;
mod events;
mod handlers;
mod model;
#[cfg(test)]
mod project_store_live;
mod store;
#[cfg(test)]
mod tests;

// Re-exported at the module root for the leader notification-delivery worker
// (`serve()`). The batch size is the env-backed `notification_delivery_batch()`
// (default `NOTIFICATION_DELIVERY_BATCH`), wired at the worker spawn.
pub(crate) use config::{notification_delivery_batch, notification_delivery_interval};
// Re-exported for the leader to bound each provider POST once it wires
// `.timeout(...)` at the reqwest client build in service/mod.rs (see the
// TODO(leader-wire) in config.rs). Unused until then, hence the allow.
#[allow(unused_imports)]
pub(crate) use config::notification_delivery_timeout;
#[cfg(feature = "http-client")]
pub(crate) use delivery::run_notification_delivery_worker_once;

use errors::notification_capability_status;

pub struct NotificationServiceImpl {
    /// Runtime handle for P4 native-entity data-plane operations. Straightforward
    /// notification CRUD persists as typed proto entities through the native
    /// catalog, neutral IR compiler, and selected backend executor.
    pub(crate) runtime: Option<Arc<DataBrokerRuntime>>,
    /// Live project registry used to reject unknown-project fallback before a
    /// direct/raw notification operation can select a physical authority.
    pub(crate) catalog: Option<Arc<CatalogManager>>,
    /// Schema-qualified outbox table (`udb_system.outbox_events`) the CDC engine
    /// tails → Apache Kafka → the Spark streaming consumer. `None` = no emit.
    pub(crate) outbox_relation: Option<String>,
    /// Per-tenant fair-admission manager (the SAME one the data plane uses via
    /// `execute_with_channel_scoped`). Mutating/listing RPCs acquire a per-tenant
    /// budget through this so one tenant can't starve the shared control plane.
    /// `None` only in bare unit-test construction (no runtime wired) —
    /// `build_notification_service` always wires it in production.
    pub(crate) channels: Option<ChannelManager>,
    pub(crate) metrics: Arc<dyn MetricsRecorder>,
}

impl NotificationServiceImpl {
    pub fn new() -> Self {
        Self {
            runtime: None,
            catalog: None,
            outbox_relation: None,
            channels: None,
            metrics: Arc::new(NoopMetrics),
        }
    }

    /// Wire the runtime used for typed native-entity notification persistence.
    pub(crate) fn with_runtime(mut self, runtime: Option<Arc<DataBrokerRuntime>>) -> Self {
        self.runtime = runtime;
        self
    }

    pub(crate) fn with_catalog(mut self, catalog: Option<Arc<CatalogManager>>) -> Self {
        self.catalog = catalog;
        self
    }

    /// Typed notification entity operations fail closed when no runtime is wired.
    pub(crate) fn require_runtime(&self) -> Result<&DataBrokerRuntime, Status> {
        self.runtime.as_deref().ok_or_else(|| {
            notification_capability_status(
                "native_entity_dispatch",
                "runtime_native_entity_dispatch",
                "notification service requires runtime native entity dispatch",
            )
        })
    }

    pub(crate) fn with_metrics(mut self, metrics: Arc<dyn MetricsRecorder>) -> Self {
        self.metrics = metrics;
        self
    }

    /// Wire the shared per-tenant fair-admission manager (same one the data plane
    /// uses) so control-plane RPCs are bounded per tenant. No-op (`None`) leaves
    /// admission disabled for bare unit-test construction.
    pub(crate) fn with_channels(mut self, channels: Option<ChannelManager>) -> Self {
        self.channels = channels;
        self
    }

    /// Wire the transactional outbox so `SendNotification` publishes a domain
    /// event to Kafka (via the CDC relay). `relation` is the schema-qualified
    /// table, e.g. `"udb_system"."outbox_events"` (`CdcConfig::outbox_relation`).
    pub(crate) fn with_outbox(mut self, relation: Option<String>) -> Self {
        self.outbox_relation = relation;
        self
    }

    /// Resolve and pin one physical Postgres authority for the request project.
    /// Empty project metadata names the explicit `default` project; it never means
    /// "pick any configured instance". Compound typed/raw operations carry the
    /// selected instance in the returned context so weighted routing cannot split
    /// a notification mutation from its transactional outbox row.
    pub(crate) fn resolve_project_store(
        &self,
        mut context: crate::RequestContext,
        write: bool,
        operation: &str,
    ) -> Result<(crate::RequestContext, PgPool), Status> {
        context.project_id = match context.project_id.trim() {
            "" => DEFAULT_PROJECT_ID.to_string(),
            project_id => project_id.to_string(),
        };
        let runtime = self.require_runtime()?;
        let catalog = self.catalog.as_deref().ok_or_else(|| {
            notification_capability_status(
                operation,
                "active_project_catalog",
                "notification direct-store operation requires the live project catalog",
            )
        })?;
        if !catalog
            .active_project_ids()
            .iter()
            .any(|active| active == &context.project_id)
        {
            return Err(notification_capability_status(
                operation,
                "active_project_catalog",
                format!(
                    "notification project '{}' has no explicitly active catalog; default-project fallback is refused",
                    context.project_id
                ),
            ));
        }
        let (pool, instance) = runtime.native_store_postgres_binding_for_service(
            "notification",
            write,
            &context,
        )?;
        context.target_backend = "postgres".to_string();
        context.target_instance = instance.unwrap_or_default();
        Ok((context, pool))
    }
}

impl Default for NotificationServiceImpl {
    fn default() -> Self {
        Self::new()
    }
}

#[tonic::async_trait]
impl NotificationService for NotificationServiceImpl {
    async fn send_notification(
        &self,
        request: Request<notif_pb::SendNotificationRequest>,
    ) -> Result<Response<notif_pb::SendNotificationResponse>, Status> {
        handlers::send_notification(self, request).await
    }

    async fn get_notification(
        &self,
        request: Request<notif_pb::GetNotificationRequest>,
    ) -> Result<Response<notif_pb::GetNotificationResponse>, Status> {
        handlers::get_notification(self, request).await
    }

    async fn list_notifications(
        &self,
        request: Request<notif_pb::ListNotificationsRequest>,
    ) -> Result<Response<notif_pb::ListNotificationsResponse>, Status> {
        handlers::list_notifications(self, request).await
    }

    async fn retry_notification(
        &self,
        request: Request<notif_pb::RetryNotificationRequest>,
    ) -> Result<Response<notif_pb::RetryNotificationResponse>, Status> {
        handlers::retry_notification(self, request).await
    }

    async fn report_delivery(
        &self,
        request: Request<notif_pb::ReportDeliveryRequest>,
    ) -> Result<Response<notif_pb::ReportDeliveryResponse>, Status> {
        handlers::report_delivery(self, request).await
    }

    async fn upsert_template(
        &self,
        request: Request<notif_pb::UpsertTemplateRequest>,
    ) -> Result<Response<notif_pb::UpsertTemplateResponse>, Status> {
        handlers::upsert_template(self, request).await
    }

    async fn get_template(
        &self,
        request: Request<notif_pb::GetTemplateRequest>,
    ) -> Result<Response<notif_pb::GetTemplateResponse>, Status> {
        handlers::get_template(self, request).await
    }

    async fn list_templates(
        &self,
        request: Request<notif_pb::ListTemplatesRequest>,
    ) -> Result<Response<notif_pb::ListTemplatesResponse>, Status> {
        handlers::list_templates(self, request).await
    }

    async fn get_delivery_stats(
        &self,
        request: Request<notif_pb::GetDeliveryStatsRequest>,
    ) -> Result<Response<notif_pb::GetDeliveryStatsResponse>, Status> {
        handlers::get_delivery_stats(self, request).await
    }

    async fn set_preference(
        &self,
        request: Request<notif_pb::SetPreferenceRequest>,
    ) -> Result<Response<notif_pb::SetPreferenceResponse>, Status> {
        handlers::set_preference(self, request).await
    }

    async fn get_preference(
        &self,
        request: Request<notif_pb::GetPreferenceRequest>,
    ) -> Result<Response<notif_pb::GetPreferenceResponse>, Status> {
        handlers::get_preference(self, request).await
    }

    async fn list_preferences(
        &self,
        request: Request<notif_pb::ListPreferencesRequest>,
    ) -> Result<Response<notif_pb::ListPreferencesResponse>, Status> {
        handlers::list_preferences(self, request).await
    }
}

impl DataBrokerService {
    /// Build the native `NotificationService`. No startup/default pool is retained:
    /// every RPC resolves and pins its request project's physical authority.
    pub(crate) fn build_notification_service(&self) -> NotificationServiceImpl {
        let runtime = self.runtime.load_full();
        let outbox = runtime.config().cdc.outbox_relation();
        let channels = Some(runtime.channels().clone());
        NotificationServiceImpl::new()
            .with_runtime(Some(runtime))
            .with_catalog(Some(self.catalog.clone()))
            .with_outbox(Some(outbox))
            .with_channels(channels)
            .with_metrics(self.metrics.clone())
    }
}
