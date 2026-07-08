//! NW3-2 — SQLite plugin.
//!
//! Same shape as the MySQL plugin. The runtime register function
//! detects `sqlite::memory:` / `:memory:` in the DSN and clamps to
//! `max_connections = 1` so the in-memory schema and data survive
//! across operations on a single shared connection — that's what
//! makes SQLite usable as the long-pending in-memory test profile.

use crate::backend::BackendKind;
use crate::backend::plugin::{Backend, RegisterCtx};

/// Zero-sized plugin for SQLite.
#[derive(Debug, Default)]
pub struct SqlitePlugin;

/// Static instance referenced by `backend::plugins::all()`.
pub static PLUGIN: SqlitePlugin = SqlitePlugin;

#[async_trait::async_trait]
impl Backend for SqlitePlugin {
    fn kind(&self) -> BackendKind {
        BackendKind::Sqlite
    }

    async fn register(&self, ctx: &mut RegisterCtx<'_>) {
        crate::runtime::core::setup_data::register_sqlite(ctx).await;
    }

    fn generate_artifacts(
        &self,
        manifest: &crate::generation::CatalogManifest,
        sql_config: &crate::generation::sql::SqlGenerationConfig,
    ) -> Result<Vec<crate::generation::GeneratedArtifact>, String> {
        crate::generation::backends::generate_sqlite_artifacts(manifest, sql_config)
            .map_err(|err| err.to_string())
    }
}

impl crate::runtime::executors::handle::DispatchFactory for SqlitePlugin {
    fn build_dispatch_executor(
        &self,
        runtime: &crate::runtime::core::DataBrokerRuntime,
        instance: Option<&str>,
        _write: bool,
        context: Option<&crate::broker::RequestContext>,
    ) -> Result<crate::runtime::executors::handle::DispatchExecutor, tonic::Status> {
        let instance_name = instance.unwrap_or("primary");
        let pool = runtime
            .sqlite_pool_for_instance(instance_name)
            .ok_or_else(|| {
                super::dispatch_instance_not_configured_status(
                    "sqlite",
                    format!(
                        "SQLite instance '{instance_name}' is not configured (set UDB_SQLITE_DSN)"
                    ),
                )
            })?
            .clone();
        // Thread the request context so generic-dispatch populates the
        // `_udb_context` table the RLS-style views read. Mirrors the PG plugin.
        let executor = match context {
            Some(ctx) => crate::runtime::executors::sqlite::SqliteExecutor::with_context(
                pool,
                std::sync::Arc::new(ctx.clone()),
            ),
            None => crate::runtime::executors::sqlite::SqliteExecutor::with_pool(pool),
        };
        Ok(crate::runtime::executors::handle::DispatchExecutor::Sqlite(
            executor,
        ))
    }
}
