//! MinIO plugin — the historical default S3-compatible object store.
//!
//! Setup is shared with the S3 plugin (`register_s3` covers both). Having
//! both plugins means `BackendKind::Minio` and `BackendKind::S3` are both
//! enumerated; `register_s3` is idempotent (it only acts once on the
//! `[minio]` config block) so duplicate invocation in the plugin loop is safe.
//! Only compiled with the `s3` Cargo feature.

use crate::backend::BackendKind;
use crate::backend::plugin::{Backend, RegisterCtx};

#[derive(Debug, Default)]
pub struct MinioPlugin;

pub static PLUGIN: MinioPlugin = MinioPlugin;

#[async_trait::async_trait]
impl Backend for MinioPlugin {
    fn kind(&self) -> BackendKind {
        BackendKind::Minio
    }

    async fn register(&self, ctx: &mut RegisterCtx<'_>) {
        crate::runtime::core::setup_data::register_s3(ctx).await;
    }

    fn generate_artifacts(
        &self,
        schemas: &[crate::ast::ProtoSchema],
        sql_config: &crate::generation::sql::SqlGenerationConfig,
    ) -> Result<Vec<crate::generation::GeneratedArtifact>, String> {
        crate::generation::backends::generate_minio_artifacts(schemas, sql_config)
            .map_err(|err| err.to_string())
    }
}

impl crate::runtime::executors::handle::DispatchFactory for MinioPlugin {
    fn build_dispatch_executor(
        &self,
        runtime: &crate::runtime::core::DataBrokerRuntime,
        instance: Option<&str>,
        _write: bool,
        _context: Option<&crate::broker::RequestContext>,
    ) -> Result<crate::runtime::executors::handle::DispatchExecutor, tonic::Status> {
        // Minio shares the S3 dispatch variant — same AWS SDK client type.
        Ok(crate::runtime::executors::handle::DispatchExecutor::S3(
            crate::runtime::executors::s3::S3Executor(runtime.s3_for_instance(instance)?.clone()),
        ))
    }
}
