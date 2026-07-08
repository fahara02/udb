//! C9 — Memcached backend plugin.
//!
//! Promotes Memcached from a metadata-only enum entry to a real
//! wired plugin backed by the canonical `memcache` crate (sync binary
//! protocol, wrapped in spawn_blocking from the executor).

use crate::backend::BackendKind;
use crate::backend::plugin::{Backend, RegisterCtx};

#[derive(Debug, Default)]
pub struct MemcachedPlugin;

pub static PLUGIN: MemcachedPlugin = MemcachedPlugin;

#[async_trait::async_trait]
impl Backend for MemcachedPlugin {
    fn kind(&self) -> BackendKind {
        BackendKind::Memcached
    }

    async fn register(&self, ctx: &mut RegisterCtx<'_>) {
        crate::runtime::core::setup_data::register_memcached(ctx).await;
    }
}

impl crate::runtime::executors::handle::DispatchFactory for MemcachedPlugin {
    fn build_dispatch_executor(
        &self,
        runtime: &crate::runtime::core::DataBrokerRuntime,
        instance: Option<&str>,
        _write: bool,
        _context: Option<&crate::broker::RequestContext>,
    ) -> Result<crate::runtime::executors::handle::DispatchExecutor, tonic::Status> {
        let instance_name = instance.unwrap_or("primary");
        let client = runtime
            .memcached_for_instance(instance_name)
            .ok_or_else(|| {
                super::dispatch_instance_not_configured_status(
                    "memcached",
                    format!(
                        "Memcached instance '{instance_name}' is not configured \
                     (set UDB_MEMCACHED_DSN)"
                    ),
                )
            })?
            .clone();
        Ok(
            crate::runtime::executors::handle::DispatchExecutor::Memcached(
                crate::runtime::executors::memcached::MemcachedExecutor::new(client),
            ),
        )
    }
}
