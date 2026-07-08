use crate::backend::BackendKind;
use crate::runtime::executors::ResourceAdminExecutor;
use crate::runtime::executors::gcs::{GcsClient, GcsExecutor};

use super::object_live_support::assert_compiled_object_tenant_isolation;
use super::support::live_ir_enabled;

const PROJECT: &str = "gcs-live";

#[tokio::test]
#[ignore = "requires UDB_IR_LIVE_GOLDEN_TESTS=1, UDB_GCS_DSN/ADC, and feature=gcs"]
async fn gcs_compiled_object_read_write_delete_match_live_objects() {
    if !live_ir_enabled() {
        eprintln!("skipping: set UDB_IR_LIVE_GOLDEN_TESTS=1 to run live IR golden tests");
        return;
    }
    let Ok(project_id) = std::env::var("UDB_GCS_DSN") else {
        eprintln!("UDB_GCS_DSN unset - skipping live GCS IR golden");
        return;
    };

    let executor = GcsExecutor::new(
        GcsClient::new(project_id)
            .await
            .expect("create GCS client from ADC"),
    );
    let bucket = format!("udb-ir-live-{}", uuid::Uuid::new_v4().simple());
    ResourceAdminExecutor::ensure_resource(&executor, &bucket, "{}")
        .await
        .expect("create live GCS bucket");
    assert_compiled_object_tenant_isolation(BackendKind::Gcs, "GCS", PROJECT, &executor, &bucket)
        .await;
    let _ = ResourceAdminExecutor::drop_resource(&executor, &bucket).await;
}
