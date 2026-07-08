//! C9 — Pinecone backend plugin.

use crate::backend::BackendKind;
use crate::backend::plugin::{Backend, RegisterCtx};

#[derive(Debug, Default)]
pub struct PineconePlugin;

pub static PLUGIN: PineconePlugin = PineconePlugin;

#[async_trait::async_trait]
impl Backend for PineconePlugin {
    fn kind(&self) -> BackendKind {
        BackendKind::Pinecone
    }
    async fn register(&self, ctx: &mut RegisterCtx<'_>) {
        crate::runtime::core::setup_data::register_pinecone(ctx).await;
    }
}

impl crate::runtime::executors::handle::DispatchFactory for PineconePlugin {
    fn build_dispatch_executor(
        &self,
        runtime: &crate::runtime::core::DataBrokerRuntime,
        instance: Option<&str>,
        _write: bool,
        _context: Option<&crate::broker::RequestContext>,
    ) -> Result<crate::runtime::executors::handle::DispatchExecutor, tonic::Status> {
        let instance_name = instance.unwrap_or("primary");
        let client = runtime
            .pinecone_for_instance(instance_name)
            .ok_or_else(|| {
                super::dispatch_instance_not_configured_status(
                    "pinecone",
                    format!(
                        "Pinecone instance '{instance_name}' not configured (set UDB_PINECONE_DSN)"
                    ),
                )
            })?
            .clone();
        Ok(
            crate::runtime::executors::handle::DispatchExecutor::Pinecone(
                crate::runtime::executors::pinecone::PineconeExecutor::new(client),
            ),
        )
    }
}
