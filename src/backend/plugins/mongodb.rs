//! MongoDB plugin.
//!
//! Delegates connection setup to `runtime::core::setup_data::register_mongodb`.
//! Only compiled with the `mongodb` Cargo feature.

use crate::backend::BackendKind;
use crate::backend::plugin::{Backend, RegisterCtx};

#[derive(Debug, Default)]
pub struct MongoDbPlugin;

pub static PLUGIN: MongoDbPlugin = MongoDbPlugin;

#[async_trait::async_trait]
impl Backend for MongoDbPlugin {
    fn kind(&self) -> BackendKind {
        BackendKind::Mongodb
    }

    async fn register(&self, ctx: &mut RegisterCtx<'_>) {
        crate::runtime::core::setup_data::register_mongodb(ctx).await;
    }

    fn generate_artifacts(
        &self,
        manifest: &crate::generation::CatalogManifest,
        sql_config: &crate::generation::sql::SqlGenerationConfig,
    ) -> Result<Vec<crate::generation::GeneratedArtifact>, String> {
        crate::generation::backends::generate_mongodb_artifacts(manifest, sql_config)
            .map_err(|err| err.to_string())
    }
}

impl crate::runtime::executors::handle::DispatchFactory for MongoDbPlugin {
    fn build_dispatch_executor(
        &self,
        runtime: &crate::runtime::core::DataBrokerRuntime,
        instance: Option<&str>,
        write: bool,
        _context: Option<&crate::broker::RequestContext>,
    ) -> Result<crate::runtime::executors::handle::DispatchExecutor, tonic::Status> {
        let routed = if write {
            instance.or_else(|| runtime.choose_instance_name("mongodb", true))
        } else {
            instance
        };
        Ok(
            crate::runtime::executors::handle::DispatchExecutor::MongoDb(
                runtime.mongodb_for_instance(routed)?.clone(),
            ),
        )
    }
}
