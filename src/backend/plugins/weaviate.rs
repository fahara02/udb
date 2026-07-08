//! C9 — Weaviate backend plugin.
//!
//! REST + GraphQL via reqwest. Same shape as the Qdrant plugin.

use crate::backend::BackendKind;
use crate::backend::plugin::{Backend, RegisterCtx};

#[derive(Debug, Default)]
pub struct WeaviatePlugin;

pub static PLUGIN: WeaviatePlugin = WeaviatePlugin;

#[async_trait::async_trait]
impl Backend for WeaviatePlugin {
    fn kind(&self) -> BackendKind {
        BackendKind::Weaviate
    }
    async fn register(&self, ctx: &mut RegisterCtx<'_>) {
        crate::runtime::core::setup_data::register_weaviate(ctx).await;
    }
}

impl crate::runtime::executors::handle::DispatchFactory for WeaviatePlugin {
    fn build_dispatch_executor(
        &self,
        runtime: &crate::runtime::core::DataBrokerRuntime,
        instance: Option<&str>,
        _write: bool,
        _context: Option<&crate::broker::RequestContext>,
    ) -> Result<crate::runtime::executors::handle::DispatchExecutor, tonic::Status> {
        let instance_name = instance.unwrap_or("primary");
        let client = runtime
            .weaviate_for_instance(instance_name)
            .ok_or_else(|| {
                super::dispatch_instance_not_configured_status(
                    "weaviate",
                    format!(
                        "Weaviate instance '{instance_name}' not configured (set UDB_WEAVIATE_DSN)"
                    ),
                )
            })?
            .clone();
        Ok(
            crate::runtime::executors::handle::DispatchExecutor::Weaviate(
                crate::runtime::executors::weaviate::WeaviateExecutor::new(client),
            ),
        )
    }
}
