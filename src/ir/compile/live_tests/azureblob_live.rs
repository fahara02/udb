use crate::backend::BackendKind;
use crate::runtime::executors::ResourceAdminExecutor;
use crate::runtime::executors::azureblob::{AzureBlobClient, AzureBlobExecutor};

use super::object_live_support::assert_compiled_object_tenant_isolation;
use super::support::live_ir_enabled;

const PROJECT: &str = "azureblob-live";

#[tokio::test]
#[ignore = "requires UDB_IR_LIVE_GOLDEN_TESTS=1, UDB_AZUREBLOB_DSN, and feature=azureblob"]
async fn azureblob_compiled_object_read_write_delete_match_live_blobs() {
    if !live_ir_enabled() {
        eprintln!("skipping: set UDB_IR_LIVE_GOLDEN_TESTS=1 to run live IR golden tests");
        return;
    }
    let Some((account, key)) = azureblob_live_credentials() else {
        eprintln!(
            "UDB_AZUREBLOB_DSN unset or missing account/key - skipping live Azure Blob IR golden"
        );
        return;
    };

    let executor = AzureBlobExecutor::new(AzureBlobClient::from_account_key(&account, &key));
    let container = format!("udb-ir-live-{}", uuid::Uuid::new_v4().simple());
    ResourceAdminExecutor::ensure_resource(&executor, &container, "{}")
        .await
        .expect("create live Azure Blob container");
    assert_compiled_object_tenant_isolation(
        BackendKind::AzureBlob,
        "Azure Blob",
        PROJECT,
        &executor,
        &container,
    )
    .await;
    let _ = ResourceAdminExecutor::drop_resource(&executor, &container).await;
}

fn azureblob_live_credentials() -> Option<(String, String)> {
    let dsn = std::env::var("UDB_AZUREBLOB_DSN").ok()?;
    crate::runtime::core::setup_data::parse_azureblob_dsn(&dsn)
}
