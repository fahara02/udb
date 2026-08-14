//! Live tests for native non-auth services.
//!
//! Keep these outside `auth_service/tests` so test ownership follows the service
//! boundary rather than the historical shared auth Postgres harness.

mod asset_image_live;
mod asset_live;
mod asset_trigger_live;
#[cfg(feature = "http-client")]
mod asset_vector_live;
mod data_plane_live;
mod data_plane_tenant_rls_live;
mod native_events_live;
mod scheduler_live;
#[cfg(feature = "http-client")]
mod search_tenant_iso_live;
mod storage_live;
#[cfg(feature = "http-client")]
mod storage_object_live;
#[cfg(feature = "http-client")]
mod storage_object_tenant_iso_live;
pub(crate) mod support;
mod webrtc_live;
mod workflow_live;
