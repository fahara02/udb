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
}

impl crate::runtime::executors::handle::DispatchFactory for MysqlPlugin {
    fn build_dispatch_executor(
        &self,
        runtime: &crate::runtime::core::DataBrokerRuntime,
        instance: Option<&str>,
        _write: bool,
        _context: Option<&crate::broker::RequestContext>,
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
        let executor = crate::runtime::executors::mysql::MysqlExecutor::with_pool(pool);
        Ok(crate::runtime::executors::handle::DispatchExecutor::Mysql(
            executor,
        ))
    }
}
