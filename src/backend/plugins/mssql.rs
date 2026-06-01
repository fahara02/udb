//! C9 — Microsoft SQL Server backend plugin.
//!
//! Real TDS-protocol implementation via the canonical `tiberius`
//! driver. Same plugin shape as the other SQL canonical stores
//! (Postgres / MySQL / SQLite): zero-sized struct + `pub static
//! PLUGIN` + `Backend` impl whose `register()` delegates to
//! `setup_data::register_mssql`, plus a `DispatchFactory` impl that
//! resolves the client by instance name.

use crate::backend::BackendKind;
use crate::backend::plugin::{Backend, RegisterCtx};

#[derive(Debug, Default)]
pub struct MssqlPlugin;

pub static PLUGIN: MssqlPlugin = MssqlPlugin;

#[async_trait::async_trait]
impl Backend for MssqlPlugin {
    fn kind(&self) -> BackendKind {
        BackendKind::Mssql
    }

    async fn register(&self, ctx: &mut RegisterCtx<'_>) {
        crate::runtime::core::setup_data::register_mssql(ctx).await;
    }

    fn generate_artifacts(
        &self,
        manifest: &crate::generation::CatalogManifest,
        sql_config: &crate::generation::sql::SqlGenerationConfig,
    ) -> Result<Vec<crate::generation::GeneratedArtifact>, String> {
        crate::generation::backends::generate_mssql_artifacts(manifest, sql_config)
            .map_err(|err| err.to_string())
    }
}

impl crate::runtime::executors::handle::DispatchFactory for MssqlPlugin {
    fn build_dispatch_executor(
        &self,
        runtime: &crate::runtime::core::DataBrokerRuntime,
        instance: Option<&str>,
        _write: bool,
        context: Option<&crate::broker::RequestContext>,
    ) -> Result<crate::runtime::executors::handle::DispatchExecutor, tonic::Status> {
        let instance_name = instance.unwrap_or("primary");
        let client = runtime
            .mssql_for_instance(instance_name)
            .ok_or_else(|| {
                tonic::Status::failed_precondition(format!(
                    "Mssql instance '{instance_name}' is not configured \
                     (set UDB_MSSQL_DSN)"
                ))
            })?
            .clone();
        // A3 (2026-05-30): if a request context is present, build
        // the context-bound executor so `sp_set_session_context`
        // gets stamped on every dispatched call. Plain `new` is
        // reserved for non-request paths (probes, health checks).
        let exec = match context {
            Some(ctx) => crate::runtime::executors::mssql::MssqlExecutor::with_context(
                client,
                std::sync::Arc::new(ctx.clone()),
            ),
            None => crate::runtime::executors::mssql::MssqlExecutor::new(client),
        };
        Ok(crate::runtime::executors::handle::DispatchExecutor::Mssql(
            exec,
        ))
    }
}
