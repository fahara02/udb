//! Postgres plugin.
//!
//! Identity comes from the trait defaults (delegated to `BackendKind`).
//! Runtime registration delegates to `runtime::core::setup_data::register_postgres`,
//! which lives in `runtime/core/` so it can reach `DataBrokerRuntime`'s private
//! pool/instance fields (§9.5).

use crate::backend::BackendKind;
use crate::backend::plugin::{Backend, RegisterCtx};

/// Zero-sized plugin for PostgreSQL.
#[derive(Debug, Default)]
pub struct PostgresPlugin;

/// Static instance referenced by `backend::plugins::all()`.
pub static PLUGIN: PostgresPlugin = PostgresPlugin;

#[async_trait::async_trait]
impl Backend for PostgresPlugin {
    fn kind(&self) -> BackendKind {
        BackendKind::Postgres
    }

    async fn register(&self, ctx: &mut RegisterCtx<'_>) {
        crate::runtime::core::setup_data::register_postgres(ctx).await;
    }
}

impl crate::runtime::executors::handle::DispatchFactory for PostgresPlugin {
    fn build_dispatch_executor(
        &self,
        runtime: &crate::runtime::core::DataBrokerRuntime,
        instance: Option<&str>,
        _write: bool,
        context: Option<&crate::broker::RequestContext>,
    ) -> Result<crate::runtime::executors::handle::DispatchExecutor, tonic::Status> {
        // Postgres generic-dispatch resolves the pool from `instance` directly;
        // replica/cache/encryption for the typed RPCs live elsewhere (§9.1).
        // U7: when a request context is present, bake it into the executor so
        // `query`/`mutate` wrap the SQL in a transaction with
        // `set_request_local_settings` applied — RLS now sees the same
        // tenant/project/purpose values on the generic-dispatch path that the
        // typed Select/Upsert RPCs install.
        let pool = runtime.pg_pool_for_instance(instance)?.clone();
        let executor = match context {
            Some(ctx) => crate::runtime::executors::postgres::PostgresExecutor::with_context(
                pool,
                std::sync::Arc::new(ctx.clone()),
            ),
            None => crate::runtime::executors::postgres::PostgresExecutor::with_pool(pool),
        };
        Ok(crate::runtime::executors::handle::DispatchExecutor::Postgres(executor))
    }
}
