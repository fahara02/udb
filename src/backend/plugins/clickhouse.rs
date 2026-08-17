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
        manifest: &crate::generation::CatalogManifest,
        sql_config: &crate::generation::sql::SqlGenerationConfig,
    ) -> Result<Vec<crate::generation::GeneratedArtifact>, String> {
        crate::generation::backends::generate_clickhouse_artifacts(manifest, sql_config)
            .map_err(|err| err.to_string())
    }
}

impl crate::runtime::executors::handle::DispatchFactory for ClickHousePlugin {
    fn build_dispatch_executor(
        &self,
        runtime: &crate::runtime::core::DataBrokerRuntime,
        instance: Option<&str>,
        write: bool,
        context: Option<&crate::broker::RequestContext>,
    ) -> Result<crate::runtime::executors::handle::DispatchExecutor, tonic::Status> {
        // With no explicit instance a write picks one. Choose among the
        // instances this project may use: the unscoped chooser can land on an
        // instance an operator labelled for a different project, and the
        // caller-supplied-instance case is already refused upstream in
        // `resolve_dispatch_executor`. An absent context yields an empty
        // project, which is the unscoped behaviour.
        let project = context.map_or("", |ctx| ctx.project_id.as_str());
        let routed = if write {
            instance
                .or_else(|| runtime.choose_instance_name_for_project("clickhouse", true, project))
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
