//! C9 — Cassandra / ScyllaDB backend plugin.

use crate::backend::BackendKind;
use crate::backend::plugin::{Backend, RegisterCtx};

#[derive(Debug, Default)]
pub struct CassandraPlugin;

pub static PLUGIN: CassandraPlugin = CassandraPlugin;

#[async_trait::async_trait]
impl Backend for CassandraPlugin {
    fn kind(&self) -> BackendKind {
        BackendKind::Cassandra
    }
    async fn register(&self, ctx: &mut RegisterCtx<'_>) {
        crate::runtime::core::setup_data::register_cassandra(ctx).await;
    }
}

impl crate::runtime::executors::handle::DispatchFactory for CassandraPlugin {
    fn build_dispatch_executor(
        &self,
        runtime: &crate::runtime::core::DataBrokerRuntime,
        instance: Option<&str>,
        _write: bool,
        _context: Option<&crate::broker::RequestContext>,
    ) -> Result<crate::runtime::executors::handle::DispatchExecutor, tonic::Status> {
        let instance_name = instance.unwrap_or("primary");
        let client = runtime
            .cassandra_for_instance(instance_name)
            .ok_or_else(|| {
                super::dispatch_instance_not_configured_status("cassandra", format!(
                    "Cassandra instance '{instance_name}' not configured (set UDB_CASSANDRA_DSN)"
                ))
            })?
            .clone();
        Ok(
            crate::runtime::executors::handle::DispatchExecutor::Cassandra(
                crate::runtime::executors::cassandra::CassandraExecutor::new(client),
            ),
        )
    }
}
