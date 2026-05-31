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
}

impl crate::runtime::executors::handle::DispatchFactory for SqlitePlugin {
    fn build_dispatch_executor(
        &self,
        runtime: &crate::runtime::core::DataBrokerRuntime,
        instance: Option<&str>,
        _write: bool,
        _context: Option<&crate::broker::RequestContext>,
    ) -> Result<crate::runtime::executors::handle::DispatchExecutor, tonic::Status> {
        let instance_name = instance.unwrap_or("primary");
        let pool = runtime
            .sqlite_pool_for_instance(instance_name)
            .ok_or_else(|| {
                tonic::Status::failed_precondition(format!(
                    "SQLite instance '{instance_name}' is not configured (set UDB_SQLITE_DSN)"
                ))
            })?
            .clone();
        let executor = crate::runtime::executors::sqlite::SqliteExecutor::with_pool(pool);
        Ok(crate::runtime::executors::handle::DispatchExecutor::Sqlite(
            executor,
        ))
    }
}
