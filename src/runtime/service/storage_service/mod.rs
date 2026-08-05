//! Native `StorageService` — proto-driven Postgres CRUD over the UDB-owned
//! `udb_storage.files` table.
//!
//! Mirrors `tenant_service`: no in-memory store, no hand-mapped schema. Table
//! and column identifiers are resolved from the embedded proto manifest via
//! [`NativeModel`] (see `runtime::native_catalog`), so the SQL here follows the
//! same single-source-of-truth rule as the rest of the native services.
//!
//! v1 is a metadata/lifecycle service; object bytes are written through the
//! native presigned URL minted at registration. Public object RPCs remain a
//! fallback only when no runtime is available to presign.
//!
//! Module layout (no god file): [`config`] the entity message + versioned outbox
//! topics + stable error-reason codes, [`errors`] the typed statuses/reason
//! trailers + field validators, [`model`] the enum<->db converters + ETag
//! normalizer + JSON→`File` decoders, [`store`] the neutral-IR query/record
//! builders + per-tenant quota lease/aggregate + durable object-GC-intent ledger,
//! [`presign`] the object-store presign/HEAD/delete helpers, [`workers`] the
//! leader-elected orphan reaper + the object-GC-intent sweep (HARD DeleteFile
//! convergence), [`handlers`] the RPCs — `mod.rs` keeps only the struct, the
//! builders/admission/require guards, and one-line trait delegators. Storage
//! lifecycle events are emitted inline via the shared
//! `native_helpers::emit_payload_event` (no dedicated events module).

use std::sync::Arc;

use sqlx::PgPool;
use tonic::{Request, Response, Status};

use crate::metrics::{MetricsRecorder, NoopMetrics};
use crate::runtime::DataBrokerRuntime;
use crate::runtime::channels::{ChannelManager, ChannelPermit, OperationChannel};

use crate::proto::udb::core::storage::services::v1 as storage_pb;
use crate::proto::udb::core::storage::services::v1::storage_service_server::StorageService;

pub use crate::proto::udb::core::storage::services::v1::storage_service_server::StorageServiceServer;

use super::DataBrokerService;
use super::native_helpers::{
    DEFAULT_OBJECT_BACKEND, DEFAULT_OBJECT_BUCKET, admit_on as native_admit_on,
    storage_object_defaults,
};

mod config;
mod errors;
mod handlers;
mod model;
mod presign;
mod store;
#[cfg(test)]
mod tests;
mod workers;

/// Postgres-backed `StorageService` handler.
#[derive(Clone)]
pub struct StorageServiceImpl {
    pub(crate) pg_pool: Option<PgPool>,
    /// Schema-qualified outbox table (`udb_system.outbox_events`) the CDC engine
    /// tails → Apache Kafka. `None` = no emit.
    pub(crate) outbox_relation: Option<String>,
    /// Broker runtime handle used to delete object bytes through the existing
    /// object executor (manifest-free `delete_object_backend_target`). `None` =
    /// metadata-only mode (no byte deletion — bytes left to lifecycle/ops).
    pub(crate) runtime: Option<Arc<DataBrokerRuntime>>,
    /// Per-tenant fair-admission manager (the SAME one the data plane uses via
    /// `execute_with_channel_scoped`). Heavy/mutating RPCs acquire a per-tenant
    /// `Object` budget through this so one tenant can't starve shared object
    /// capacity. `None` only in metadata-only/test construction (no runtime
    /// wired) — in production `build_storage_service` always wires it.
    pub(crate) channels: Option<ChannelManager>,
    /// Object-store backend + bucket native storage owns its bytes in.
    pub(crate) object_backend: String,
    pub(crate) object_bucket: String,
    pub(crate) metrics: Arc<dyn MetricsRecorder>,
}

impl StorageServiceImpl {
    pub fn new() -> Self {
        Self {
            pg_pool: None,
            outbox_relation: None,
            runtime: None,
            channels: None,
            object_backend: DEFAULT_OBJECT_BACKEND.to_string(),
            object_bucket: DEFAULT_OBJECT_BUCKET.to_string(),
            metrics: Arc::new(NoopMetrics),
        }
    }

    pub fn with_postgres(mut self, pool: Option<PgPool>) -> Self {
        self.pg_pool = pool;
        self
    }

    pub(crate) fn with_metrics(mut self, metrics: Arc<dyn MetricsRecorder>) -> Self {
        self.metrics = metrics;
        self
    }

    /// Wire the runtime handle + object-store target so `DeleteFile`/reaper can
    /// remove object bytes (not just metadata) via the shared object executor.
    pub(crate) fn with_object(
        mut self,
        runtime: Option<Arc<DataBrokerRuntime>>,
        backend: String,
        bucket: String,
    ) -> Self {
        // Capture the shared per-tenant fair-admission manager so mutating RPCs
        // can acquire the per-tenant Object budget (same path as the data plane).
        self.channels = runtime.as_ref().map(|rt| rt.channels().clone());
        self.runtime = runtime;
        if !backend.trim().is_empty() {
            self.object_backend = backend;
        }
        if !bucket.trim().is_empty() {
            self.object_bucket = bucket;
        }
        self
    }

    /// Wire the transactional outbox so storage lifecycle events publish domain
    /// events to Kafka (via the CDC relay). `relation` is the schema-qualified
    /// table, e.g. `"udb_system"."outbox_events"` (`CdcConfig::outbox_relation`).
    pub(crate) fn with_outbox(mut self, relation: Option<String>) -> Self {
        self.outbox_relation = relation;
        self
    }

    /// File metadata CRUD is durable-only: fail closed when no Postgres pool exists.
    /// Typed File entities persist through native-entity dispatch on the backend
    /// bound to this service (extend_udb.md P4) — not the hardcoded PG pool. Fail
    /// closed when no runtime is wired (bare metadata-only/test construction).
    pub(crate) fn require_runtime(&self) -> Result<&DataBrokerRuntime, Status> {
        self.runtime.as_deref().ok_or_else(|| {
            errors::storage_capability_status(
                "native_entity_dispatch",
                "runtime_native_entity_dispatch",
                "storage service requires runtime native entity dispatch",
            )
        })
    }

    /// Per-tenant fair admission for a mutating/heavy storage RPC. Acquires the
    /// shared `Object` channel budget SCOPED to the validated tenant (+ project),
    /// so a single tenant's flood cannot starve other tenants' object capacity —
    /// the exact path the data plane uses via `execute_with_channel_scoped`.
    ///
    /// On budget/concurrency exhaustion this returns the same
    /// `Status::resource_exhausted` backpressure the data plane returns. The
    /// returned [`ChannelPermit`] must be held for the duration of the RPC (drop
    /// = release). `None` channels (no runtime wired — metadata-only/test mode)
    /// admit without a permit since there is no shared object work to starve.
    ///
    /// `tenant` MUST be the VALIDATED tenant (post `validate_request_*`), never an
    /// unverified body field.
    pub(crate) async fn admit(
        &self,
        tenant: &str,
        project: &str,
    ) -> Result<Option<ChannelPermit>, Status> {
        native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "storage",
            OperationChannel::Object,
            tenant,
            Some(project),
        )
        .await
    }

    /// Lighter per-tenant fair admission for a READ RPC (`get_file`/`list_files`).
    /// Acquires the cheap `Read` channel budget scoped to the validated tenant so
    /// one tenant cannot exhaust the shared pool with reads, without charging the
    /// heavier `Object` cost a mutating/object-touching RPC pays.
    pub(crate) async fn admit_read(&self, tenant: &str) -> Result<Option<ChannelPermit>, Status> {
        native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "storage",
            OperationChannel::Read,
            tenant,
            Some(""),
        )
        .await
    }
}

impl Default for StorageServiceImpl {
    fn default() -> Self {
        Self::new()
    }
}

#[tonic::async_trait]
impl StorageService for StorageServiceImpl {
    async fn register_upload(
        &self,
        request: Request<storage_pb::RegisterUploadRequest>,
    ) -> Result<Response<storage_pb::RegisterUploadResponse>, Status> {
        handlers::register_upload(self, request).await
    }

    async fn finalize_upload(
        &self,
        request: Request<storage_pb::FinalizeUploadRequest>,
    ) -> Result<Response<storage_pb::FinalizeUploadResponse>, Status> {
        handlers::finalize_upload(self, request).await
    }

    async fn get_download_url(
        &self,
        request: Request<storage_pb::GetDownloadUrlRequest>,
    ) -> Result<Response<storage_pb::GetDownloadUrlResponse>, Status> {
        handlers::get_download_url(self, request).await
    }

    async fn reissue_upload_url(
        &self,
        request: Request<storage_pb::ReissueUploadUrlRequest>,
    ) -> Result<Response<storage_pb::ReissueUploadUrlResponse>, Status> {
        handlers::reissue_upload_url(self, request).await
    }

    type DownloadFileStream = handlers::DownloadFileStream;

    async fn download_file(
        &self,
        request: Request<storage_pb::DownloadFileRequest>,
    ) -> Result<Response<Self::DownloadFileStream>, Status> {
        handlers::download_file(self, request).await
    }

    async fn get_file(
        &self,
        request: Request<storage_pb::GetFileRequest>,
    ) -> Result<Response<storage_pb::GetFileResponse>, Status> {
        handlers::get_file(self, request).await
    }

    async fn update_file(
        &self,
        request: Request<storage_pb::UpdateFileRequest>,
    ) -> Result<Response<storage_pb::UpdateFileResponse>, Status> {
        handlers::update_file(self, request).await
    }

    async fn delete_file(
        &self,
        request: Request<storage_pb::DeleteFileRequest>,
    ) -> Result<Response<storage_pb::DeleteFileResponse>, Status> {
        handlers::delete_file(self, request).await
    }

    async fn list_files(
        &self,
        request: Request<storage_pb::ListFilesRequest>,
    ) -> Result<Response<storage_pb::ListFilesResponse>, Status> {
        handlers::list_files(self, request).await
    }
}

impl DataBrokerService {
    /// Build the native `StorageService`, wired to the broker's Postgres pool
    /// and the transactional outbox, and spawn the periodic orphan reaper.
    pub(crate) fn build_storage_service(&self) -> StorageServiceImpl {
        let runtime = self.runtime.load_full();
        // Native-service persistence resolves through the discovery seam (extend_udb.md):
        // the backend is read from this service's proto `native_service` binding, then a
        // health/weight-routed instance is chosen — not the process-global pool.
        let pg_pool = runtime
            .native_store_pool_for_service("storage", true, "")
            .ok();
        let outbox = runtime.config().cdc.outbox_relation();
        let (object_backend, object_bucket) = storage_object_defaults(
            std::env::var("UDB_STORAGE_OBJECT_BACKEND").ok(),
            std::env::var("UDB_STORAGE_BUCKET").ok(),
        );
        let svc = StorageServiceImpl::new()
            .with_postgres(pg_pool)
            .with_outbox(Some(outbox))
            .with_metrics(self.metrics.clone())
            .with_object(Some(runtime.clone()), object_backend, object_bucket);

        // Periodic orphan reaper: hard-delete bounded batches of `PENDING` files
        // that were registered but never finalized. Interval/age/batch are
        // env-tunable; interval/age set to 0 disables the reaper. Best-effort —
        // failures are logged only.
        let interval_secs = std::env::var("UDB_STORAGE_REAP_INTERVAL_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(3600);
        let orphan_age_minutes = std::env::var("UDB_STORAGE_ORPHAN_AGE_MINUTES")
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(1440);
        let orphan_batch_size = std::env::var("UDB_STORAGE_ORPHAN_BATCH_SIZE")
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(500);
        if interval_secs > 0 && orphan_age_minutes > 0 && svc.pg_pool.is_some() {
            let reaper = svc.clone();
            let singleton_pool = svc.pg_pool.clone().expect("checked above");
            let singleton_relation = runtime.config().cdc.lock_log_relation();
            crate::runtime::service::native_runtime::NativeWorkerHost::spawn_while_leader(
                crate::runtime::singleton::WORKER_STORAGE_ORPHAN_REAPER,
                "storage orphan reaper deleted PENDING files",
                singleton_pool,
                singleton_relation,
                std::time::Duration::from_secs(interval_secs),
                move || {
                    let reaper_once = reaper.clone();
                    async move {
                        reaper_once
                            .reap_orphans(orphan_age_minutes, orphan_batch_size)
                            .await
                            .map(|n| n as i64)
                    }
                },
            );
        }

        // Leader-elected GC-intent sweep: drive PENDING durable object-GC intents
        // (recorded by HARD DeleteFile) to convergence, dead-lettering after the
        // attempt cap. Held under its own lease so it runs independently of the
        // orphan reaper (which can be disabled or run on a slower interval).
        // Interval env-tunable; 0 disables. Best-effort — failures are logged only.
        let gc_interval_secs = std::env::var("UDB_STORAGE_GC_SWEEP_INTERVAL_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(300);
        let gc_batch_size = std::env::var("UDB_STORAGE_GC_SWEEP_BATCH_SIZE")
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(200);
        if gc_interval_secs > 0 && svc.pg_pool.is_some() {
            let sweeper = svc.clone();
            let gc_pool = svc.pg_pool.clone().expect("checked above");
            let gc_relation = runtime.config().cdc.lock_log_relation();
            crate::runtime::service::native_runtime::NativeWorkerHost::spawn_while_leader(
                config::WORKER_STORAGE_GC_SWEEP,
                "storage GC-intent sweep converged object deletions",
                gc_pool,
                gc_relation,
                std::time::Duration::from_secs(gc_interval_secs),
                move || {
                    let sweeper_once = sweeper.clone();
                    async move {
                        sweeper_once
                            .sweep_gc_intents(gc_batch_size)
                            .await
                            .map(|n| n as i64)
                    }
                },
            );
        }

        svc
    }
}
