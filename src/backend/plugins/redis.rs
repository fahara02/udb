//! Redis plugin.
//!
//! Delegates connection setup to `runtime::core::setup_data::register_redis`
//! (which has descendant-module access to `DataBrokerRuntime`'s private
//! `redis*` fields per §9.5). Only compiled with the `redis` Cargo feature.

use crate::backend::BackendKind;
use crate::backend::plugin::{Backend, RegisterCtx};

#[derive(Debug, Default)]
pub struct RedisPlugin;

pub static PLUGIN: RedisPlugin = RedisPlugin;

#[async_trait::async_trait]
impl Backend for RedisPlugin {
    fn kind(&self) -> BackendKind {
        BackendKind::Redis
    }

    async fn register(&self, ctx: &mut RegisterCtx<'_>) {
        crate::runtime::core::setup_data::register_redis(ctx).await;
    }

    fn generate_artifacts(
        &self,
        manifest: &crate::generation::CatalogManifest,
        sql_config: &crate::generation::sql::SqlGenerationConfig,
    ) -> Result<Vec<crate::generation::GeneratedArtifact>, String> {
        crate::generation::backends::generate_redis_artifacts(manifest, sql_config)
            .map_err(|err| err.to_string())
    }
}

impl crate::runtime::executors::handle::DispatchFactory for RedisPlugin {
    fn build_dispatch_executor(
        &self,
        runtime: &crate::runtime::core::DataBrokerRuntime,
        instance: Option<&str>,
        _write: bool,
        _context: Option<&crate::broker::RequestContext>,
    ) -> Result<crate::runtime::executors::handle::DispatchExecutor, tonic::Status> {
        Ok(crate::runtime::executors::handle::DispatchExecutor::Redis(
            crate::runtime::executors::redis::RedisExecutor::new(
                runtime.redis_for_instance(instance)?.clone(),
            ),
        ))
    }
}
