//! S3 plugin.
//!
//! Shares the `register_s3` setup with the MinIO plugin (both use the
//! `[minio]` config block — historically the default S3-compatible store).
//! Only compiled with the `s3` Cargo feature.

use crate::backend::BackendKind;
use crate::backend::plugin::{Backend, RegisterCtx};

#[derive(Debug, Default)]
pub struct S3Plugin;

pub static PLUGIN: S3Plugin = S3Plugin;

#[async_trait::async_trait]
impl Backend for S3Plugin {
    fn kind(&self) -> BackendKind {
        BackendKind::S3
    }

    async fn register(&self, ctx: &mut RegisterCtx<'_>) {
        crate::runtime::core::setup_data::register_s3(ctx).await;
    }
}

impl crate::runtime::executors::handle::DispatchFactory for S3Plugin {
    fn build_dispatch_executor(
        &self,
        runtime: &crate::runtime::core::DataBrokerRuntime,
        instance: Option<&str>,
        _write: bool,
        _context: Option<&crate::broker::RequestContext>,
    ) -> Result<crate::runtime::executors::handle::DispatchExecutor, tonic::Status> {
        // S3 object store: no write-routing (the AWS SDK handles its own
        // endpoint selection); instance simply names the configured client.
        Ok(crate::runtime::executors::handle::DispatchExecutor::S3(
            crate::runtime::executors::s3::S3Executor(runtime.s3_for_instance(instance)?.clone()),
        ))
    }
}
