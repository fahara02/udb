//! C9 — Google Cloud Storage backend plugin.

use crate::backend::BackendKind;
use crate::backend::plugin::{Backend, RegisterCtx};

#[derive(Debug, Default)]
pub struct GcsPlugin;

pub static PLUGIN: GcsPlugin = GcsPlugin;

#[async_trait::async_trait]
impl Backend for GcsPlugin {
    fn kind(&self) -> BackendKind {
        BackendKind::Gcs
    }
    async fn register(&self, ctx: &mut RegisterCtx<'_>) {
        crate::runtime::core::setup_data::register_gcs(ctx).await;
    }
}

impl crate::runtime::executors::handle::DispatchFactory for GcsPlugin {
    fn build_dispatch_executor(
        &self,
        runtime: &crate::runtime::core::DataBrokerRuntime,
        instance: Option<&str>,
        _write: bool,
        _context: Option<&crate::broker::RequestContext>,
    ) -> Result<crate::runtime::executors::handle::DispatchExecutor, tonic::Status> {
        let instance_name = instance.unwrap_or("primary");
        let client = runtime
            .gcs_for_instance(instance_name)
            .ok_or_else(|| {
                super::dispatch_instance_not_configured_status(
                    "gcs",
                    format!("GCS instance '{instance_name}' not configured (set UDB_GCS_DSN)"),
                )
            })?
            .clone();
        Ok(crate::runtime::executors::handle::DispatchExecutor::Gcs(
            crate::runtime::executors::gcs::GcsExecutor::new(client),
        ))
    }
}
