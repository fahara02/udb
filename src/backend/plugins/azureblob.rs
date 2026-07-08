//! C9 — Azure Blob Storage backend plugin.

use crate::backend::BackendKind;
use crate::backend::plugin::{Backend, RegisterCtx};

#[derive(Debug, Default)]
pub struct AzureBlobPlugin;

pub static PLUGIN: AzureBlobPlugin = AzureBlobPlugin;

#[async_trait::async_trait]
impl Backend for AzureBlobPlugin {
    fn kind(&self) -> BackendKind {
        BackendKind::AzureBlob
    }
    async fn register(&self, ctx: &mut RegisterCtx<'_>) {
        crate::runtime::core::setup_data::register_azureblob(ctx).await;
    }
}

impl crate::runtime::executors::handle::DispatchFactory for AzureBlobPlugin {
    fn build_dispatch_executor(
        &self,
        runtime: &crate::runtime::core::DataBrokerRuntime,
        instance: Option<&str>,
        _write: bool,
        _context: Option<&crate::broker::RequestContext>,
    ) -> Result<crate::runtime::executors::handle::DispatchExecutor, tonic::Status> {
        let instance_name = instance.unwrap_or("primary");
        let client = runtime
            .azureblob_for_instance(instance_name)
            .ok_or_else(|| {
                super::dispatch_instance_not_configured_status("azureblob", format!(
                    "Azure Blob instance '{instance_name}' not configured (set UDB_AZUREBLOB_DSN)"
                ))
            })?
            .clone();
        Ok(
            crate::runtime::executors::handle::DispatchExecutor::AzureBlob(
                crate::runtime::executors::azureblob::AzureBlobExecutor::new(client),
            ),
        )
    }
}
