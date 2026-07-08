use crate::backend::BackendKind;
use crate::runtime::executors::ResourceAdminExecutor;
use crate::runtime::executors::object_stream_live_tests::s3_executor_from_env;

use super::object_live_support::assert_compiled_object_tenant_isolation;
use super::support::live_ir_enabled;
const PROJECT: &str = "s3-live";

#[tokio::test]
#[ignore = "requires UDB_IR_LIVE_GOLDEN_TESTS=1, live S3/MinIO env, and feature=s3"]
async fn s3_compiled_object_read_write_delete_match_live_objects() {
    if !live_ir_enabled() {
        eprintln!("skipping: set UDB_IR_LIVE_GOLDEN_TESTS=1 to run live IR golden tests");
        return;
    }

    let executor = s3_executor_from_env();
    let bucket = format!("udb-ir-live-{}", uuid::Uuid::new_v4().simple());

    ResourceAdminExecutor::ensure_resource(&executor, &bucket, "{}")
        .await
        .expect("create live S3 bucket");
    assert_compiled_object_tenant_isolation(BackendKind::S3, "S3", PROJECT, &executor, &bucket)
        .await;
    let _ = ResourceAdminExecutor::drop_resource(&executor, &bucket).await;
}
