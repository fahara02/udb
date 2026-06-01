//! NW3-1 — MySQL plugin.
//!
//! Mirrors the Postgres plugin shape. Runtime registration delegates
//! to `runtime::core::setup_data::register_mysql`, which reads
//! `UDB_MYSQL_DSN`, opens a sqlx-mysql pool, stores it on the
//! runtime, and registers a `MysqlCanonicalStore` in the
//! `CanonicalStoreRegistry` so projection / saga / admin audit /
//! migration audit routes work against MySQL out of the box.

use crate::backend::BackendKind;
use crate::backend::plugin::{Backend, RegisterCtx};

/// Zero-sized plugin for MySQL.
#[derive(Debug, Default)]
pub struct MysqlPlugin;

/// Static instance referenced by `backend::plugins::all()`.
pub static PLUGIN: MysqlPlugin = MysqlPlugin;

#[async_trait::async_trait]
impl Backend for MysqlPlugin {
    fn kind(&self) -> BackendKind {
        BackendKind::Mysql
    }

    async fn register(&self, ctx: &mut RegisterCtx<'_>) {
        crate::runtime::core::setup_data::register_mysql(ctx).await;
    }

    fn generate_artifacts(
        &self,
        manifest: &crate::generation::CatalogManifest,
        sql_config: &crate::generation::sql::SqlGenerationConfig,
    ) -> Result<Vec<crate::generation::GeneratedArtifact>, String> {
        crate::generation::backends::generate_mysql_artifacts(manifest, sql_config)
            .map_err(|err| err.to_string())
    }
}

impl crate::runtime::executors::handle::DispatchFactory for MysqlPlugin {
    fn build_dispatch_executor(
        &self,
        runtime: &crate::runtime::core::DataBrokerRuntime,
        instance: Option<&str>,
        _write: bool,
        context: Option<&crate::broker::RequestContext>,
    ) -> Result<crate::runtime::executors::handle::DispatchExecutor, tonic::Status> {
        // Resolve the MySQL pool for the requested instance. `None`
        // falls back to "primary"; the runtime registers `primary`
        // during `register_mysql` if `UDB_MYSQL_DSN` was set.
        let instance_name = instance.unwrap_or("primary");
        let pool = runtime
            .mysql_pool_for_instance(instance_name)
            .ok_or_else(|| {
                tonic::Status::failed_precondition(format!(
                    "MySQL instance '{instance_name}' is not configured (set UDB_MYSQL_DSN)"
                ))
            })?
            .clone();
        // Thread the request context so generic-dispatch `query`/`mutate` set the
        // `app.current_*` session vars (matching the typed RPC path); otherwise a
        // pooled connection runs tenant-blind. Mirrors the Postgres plugin.
        let executor = match context {
            Some(ctx) => crate::runtime::executors::mysql::MysqlExecutor::with_context(
                pool,
                std::sync::Arc::new(ctx.clone()),
            ),
            None => crate::runtime::executors::mysql::MysqlExecutor::with_pool(pool),
        };
        Ok(crate::runtime::executors::handle::DispatchExecutor::Mysql(
            executor,
        ))
    }
}
