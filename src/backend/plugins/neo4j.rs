//! Neo4j plugin.
//!
//! Delegates connection setup to `runtime::core::setup_data::register_neo4j`.
//! Only compiled with the `neo4j` Cargo feature.

use crate::backend::BackendKind;
use crate::backend::plugin::{Backend, RegisterCtx};

#[derive(Debug, Default)]
pub struct Neo4jPlugin;

pub static PLUGIN: Neo4jPlugin = Neo4jPlugin;

#[async_trait::async_trait]
impl Backend for Neo4jPlugin {
    fn kind(&self) -> BackendKind {
        BackendKind::Neo4j
    }

    async fn register(&self, ctx: &mut RegisterCtx<'_>) {
        crate::runtime::core::setup_data::register_neo4j(ctx).await;
    }

    fn generate_artifacts(
        &self,
        schemas: &[crate::ast::ProtoSchema],
        sql_config: &crate::generation::sql::SqlGenerationConfig,
    ) -> Result<Vec<crate::generation::GeneratedArtifact>, String> {
        crate::generation::backends::generate_neo4j_artifacts(schemas, sql_config)
            .map_err(|err| err.to_string())
    }
}

impl crate::runtime::executors::handle::DispatchFactory for Neo4jPlugin {
    fn build_dispatch_executor(
        &self,
        runtime: &crate::runtime::core::DataBrokerRuntime,
        instance: Option<&str>,
        write: bool,
        _context: Option<&crate::broker::RequestContext>,
    ) -> Result<crate::runtime::executors::handle::DispatchExecutor, tonic::Status> {
        let routed = if write {
            instance.or_else(|| runtime.choose_instance_name("neo4j", true))
        } else {
            instance
        };
        Ok(crate::runtime::executors::handle::DispatchExecutor::Neo4j(
            runtime.neo4j_for_instance(routed)?.clone(),
        ))
    }
}
