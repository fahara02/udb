//! Live tests for native non-auth services.
//!
//! Keep these outside `auth_service/tests` so test ownership follows the service
//! boundary rather than the historical shared auth Postgres harness.

mod asset_image_live;
mod asset_live;
mod asset_trigger_live;
#[cfg(feature = "http-client")]
mod asset_vector_live;
mod audit_sink_live;
mod authz_deny_path_live;
mod backup_live;
mod catalog_authority_live;
#[cfg(feature = "kafka")]
mod cdc_consumer_live;
#[cfg(feature = "redis")]
mod data_cache_redaction_live;
mod data_contract_live;
mod data_error_matrix_live;
mod data_plane_live;
mod data_plane_seam_live;
mod data_plane_tenant_rls_live;
mod data_revision_live;
#[cfg(feature = "kafka")]
mod livequery_journal_live;
mod native_events_live;
mod native_worker_seams_live;
#[cfg(feature = "http-client")]
pub(super) mod notification_http_live;
pub(super) mod ops_seams_live;
mod projection_drift_live;
#[cfg(feature = "redis")]
mod rate_limit_live;
mod scheduler_live;
#[cfg(feature = "http-client")]
mod search_tenant_iso_live;
mod storage_live;
#[cfg(feature = "http-client")]
mod storage_object_live;
#[cfg(feature = "http-client")]
mod storage_object_tenant_iso_live;
#[cfg(feature = "http-client")]
mod storage_seams_live;
mod store_rpc_live;
pub(crate) mod support;
// C1/C5/C6/C7: Elasticsearch / Weaviate / Pinecone-stub vector isolation.
#[cfg(all(
    feature = "http-client",
    feature = "qdrant",
    feature = "elasticsearch",
    feature = "weaviate",
    feature = "pinecone"
))]
mod vector_backends_live;
// C2/C3/C8: served Qdrant vector RPC isolation on persisted routes.
#[cfg(all(feature = "http-client", feature = "qdrant"))]
mod vector_tenant_iso_live;
#[cfg(feature = "http-client")]
mod webhook_delivery_live;
mod webrtc_live;
mod wire_types_live;
mod workflow_live;
