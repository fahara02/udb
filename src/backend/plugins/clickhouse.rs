//! ClickHouse plugin.
//!
//! Delegates connection setup to `runtime::core::setup_data::register_clickhouse`.
//! Only compiled with the `clickhouse` Cargo feature.

use crate::backend::BackendKind;
use crate::backend::plugin::{Backend, RegisterCtx};

#[derive(Debug, Default)]
pub struct ClickHousePlugin;

pub static PLUGIN: ClickHousePlugin = ClickHousePlugin;

#[async_trait::async_trait]
impl Backend for ClickHousePlugin {
    fn kind(&self) -> BackendKind {
        BackendKind::Clickhouse
    }

    async fn register(&self, ctx: &mut RegisterCtx<'_>) {
        crate::runtime::core::setup_data::register_clickhouse(ctx).await;
    }

    fn generate_artifacts(
        &self,
        schemas: &[crate::ast::ProtoSchema],
        sql_config: &crate::generation::sql::SqlGenerationConfig,
    ) -> Result<Vec<crate::generation::GeneratedArtifact>, String> {
        crate::generation::backends::generate_clickhouse_artifacts(schemas, sql_config)
            .map_err(|err| err.to_string())
    }
}

impl crate::runtime::executors::handle::DispatchFactory for ClickHousePlugin {
    fn build_dispatch_executor(
        &self,
        runtime: &crate::runtime::core::DataBrokerRuntime,
        instance: Option<&str>,
        write: bool,
        _context: Option<&crate::broker::RequestContext>,
    ) -> Result<crate::runtime::executors::handle::DispatchExecutor, tonic::Status> {
        let routed = if write {
            instance.or_else(|| runtime.choose_instance_name("clickhouse", true))
        } else {
            instance
        };
        Ok(
            crate::runtime::executors::handle::DispatchExecutor::ClickHouse(
                runtime.clickhouse_for_instance(routed)?.clone(),
            ),
        )
    }
}
