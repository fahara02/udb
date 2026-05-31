//! Qdrant plugin.
//!
//! Delegates connection setup to `runtime::core::setup_data::register_qdrant`.
//! Only compiled with the `qdrant` Cargo feature.

use crate::backend::BackendKind;
use crate::backend::plugin::{Backend, RegisterCtx};

#[derive(Debug, Default)]
pub struct QdrantPlugin;

pub static PLUGIN: QdrantPlugin = QdrantPlugin;

#[async_trait::async_trait]
impl Backend for QdrantPlugin {
    fn kind(&self) -> BackendKind {
        BackendKind::Qdrant
    }

    async fn register(&self, ctx: &mut RegisterCtx<'_>) {
        crate::runtime::core::setup_data::register_qdrant(ctx).await;
    }

    fn generate_artifacts(
        &self,
        schemas: &[crate::ast::ProtoSchema],
        sql_config: &crate::generation::sql::SqlGenerationConfig,
    ) -> Result<Vec<crate::generation::GeneratedArtifact>, String> {
        crate::generation::backends::generate_qdrant_artifacts(schemas, sql_config)
            .map_err(|err| err.to_string())
    }
}

impl crate::runtime::executors::handle::DispatchFactory for QdrantPlugin {
    fn build_dispatch_executor(
        &self,
        runtime: &crate::runtime::core::DataBrokerRuntime,
        instance: Option<&str>,
        write: bool,
        _context: Option<&crate::broker::RequestContext>,
    ) -> Result<crate::runtime::executors::handle::DispatchExecutor, tonic::Status> {
        let routed = if write {
            instance.or_else(|| runtime.choose_instance_name("qdrant", true))
        } else {
            instance
        };
        Ok(crate::runtime::executors::handle::DispatchExecutor::Qdrant(
            crate::runtime::executors::qdrant::QdrantExecutor(
                runtime.qdrant_for_instance(routed)?.clone(),
            ),
        ))
    }
}
