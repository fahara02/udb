//! C9 — Elasticsearch backend plugin.
//!
//! Promotes Elasticsearch from a metadata-only enum entry to a real
//! wired plugin. Mirrors the Qdrant / Mongo plugin shape: zero-sized
//! struct + `pub static PLUGIN` + `Backend` impl whose `register()`
//! delegates to `setup_data::register_elasticsearch`, plus a
//! `DispatchFactory` impl that builds an `ElasticsearchExecutor` for
//! the resolved instance.

use crate::backend::BackendKind;
use crate::backend::plugin::{Backend, RegisterCtx};

#[derive(Debug, Default)]
pub struct ElasticsearchPlugin;

pub static PLUGIN: ElasticsearchPlugin = ElasticsearchPlugin;

#[async_trait::async_trait]
impl Backend for ElasticsearchPlugin {
    fn kind(&self) -> BackendKind {
        BackendKind::Elasticsearch
    }

    async fn register(&self, ctx: &mut RegisterCtx<'_>) {
        crate::runtime::core::setup_data::register_elasticsearch(ctx).await;
    }
}

impl crate::runtime::executors::handle::DispatchFactory for ElasticsearchPlugin {
    fn build_dispatch_executor(
        &self,
        runtime: &crate::runtime::core::DataBrokerRuntime,
        instance: Option<&str>,
        _write: bool,
        _context: Option<&crate::broker::RequestContext>,
    ) -> Result<crate::runtime::executors::handle::DispatchExecutor, tonic::Status> {
        let instance_name = instance.unwrap_or("primary");
        let client = runtime
            .elasticsearch_for_instance(instance_name)
            .ok_or_else(|| {
                super::dispatch_instance_not_configured_status(
                    "elasticsearch",
                    format!(
                        "Elasticsearch instance '{instance_name}' is not configured \
                     (set UDB_ELASTIC_DSN)"
                    ),
                )
            })?
            .clone();
        Ok(
            crate::runtime::executors::handle::DispatchExecutor::Elasticsearch(
                crate::runtime::executors::elasticsearch::ElasticsearchExecutor::new(client),
            ),
        )
    }
}
