//! Azure Blob Storage executor (C9). Real SDK via the official
//! `azure_storage_blobs` crate.
//!
//! ## Dispatch
//!
//! The compiler emits `CompiledRendering::Object` carrying
//! `(bucket, key, op)`. The runtime turns that into a JSON dispatch
//! request — same shape as the S3 executor:
//!
//! ```json
//! { "op": "get|put|delete|list",
//!   "container": "<name>",
//!   "blob": "<key>",
//!   "content_type": "..." }
//! ```
//!
//! Auth: `azure_storage::prelude::StorageCredentials::access_key`
//! (account key). Other forms (AAD, SAS) are operator-controlled at
//! the SDK level; this executor takes the parsed credentials.

use std::sync::Arc;

use azure_storage::prelude::*;
use azure_storage_blobs::prelude::*;

use crate::runtime::backend_context::{
    AppliedContext, BackendContextEnforcer, ContextEffect, enforce_with_mechanism,
};
use crate::runtime::executor_utils::{build_probe, parse_object_dispatch, reject_oversized_object};
use crate::runtime::executors::{
    BackendExecutor, BackendHealth, BackendProbe, MutationExecutor, ObjectExecutor, QueryExecutor,
    ResourceAdminExecutor, SearchExecutor,
};

#[derive(Clone)]
pub struct AzureBlobClient {
    inner: Arc<BlobServiceClient>,
}

impl std::fmt::Debug for AzureBlobClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AzureBlobClient").finish()
    }
}

impl AzureBlobClient {
    /// Construct from an account name + access key. The DSN parsed
    /// at register time supplies both.
    pub fn from_account_key(account: &str, key: &str) -> Self {
        let credentials = StorageCredentials::access_key(account.to_string(), key.to_string());
        let svc = BlobServiceClient::new(account, credentials);
        Self {
            inner: Arc::new(svc),
        }
    }

    pub async fn ping(&self) -> Result<(), String> {
        // List containers (small page) — cheapest call that exercises
        // both transport + auth.
        use futures::StreamExt;
        let mut stream = self.inner.list_containers().into_stream();
        if let Some(res) = stream.next().await {
            res.map(|_| ())
                .map_err(|e| format!("azure blob ping failed: {e}"))
        } else {
            Ok(())
        }
    }

    pub fn container(&self, name: &str) -> ContainerClient {
        self.inner.container_client(name.to_string())
    }
}

#[derive(Debug, Clone)]
pub struct AzureBlobExecutor {
    client: AzureBlobClient,
}

impl AzureBlobExecutor {
    pub fn new(client: AzureBlobClient) -> Self {
        Self { client }
    }
}

impl BackendContextEnforcer for AzureBlobExecutor {
    fn backend_label(&self) -> &str {
        "azureblob"
    }
    fn enforce(&self, ctx: &AppliedContext) -> ContextEffect {
        enforce_with_mechanism(
            ctx,
            "key prefix t:<tenant>/p:<project>/ prepended by compile_read/write/delete",
        )
    }
}

impl BackendHealth for AzureBlobExecutor {
    async fn ping(&self) -> Result<(), String> {
        self.client.ping().await
    }
}

/// Azure-Blob object-dispatch parse: container primary
/// (`container`/`bucket`), blob as `blob`/`key`. Delegates to the shared
/// object-store parser.
fn parse_dispatch(req: &str) -> Result<(String, String, String, Option<String>), tonic::Status> {
    parse_object_dispatch(
        req,
        &["container", "bucket"],
        &["blob", "key"],
        "container`/`bucket",
    )
}

impl QueryExecutor for AzureBlobExecutor {
    async fn query(&self, _req: &str) -> Result<String, tonic::Status> {
        Err(tonic::Status::failed_precondition(
            "UDB_UNSUPPORTED_OPERATION: Azure Blob has no query surface; use get_object",
        ))
    }
}

impl MutationExecutor for AzureBlobExecutor {
    async fn mutate(&self, _req: &str) -> Result<String, tonic::Status> {
        Err(tonic::Status::failed_precondition(
            "UDB_UNSUPPORTED_OPERATION: Azure Blob has no mutation surface; use put_object",
        ))
    }
}

impl SearchExecutor for AzureBlobExecutor {
    async fn search(&self, _: &str) -> Result<String, tonic::Status> {
        Err(tonic::Status::failed_precondition(
            "UDB_UNSUPPORTED_OPERATION: Azure Blob is not searchable",
        ))
    }
}

impl ObjectExecutor for AzureBlobExecutor {
    async fn get_object(&self, request_json: &str) -> Result<Vec<u8>, tonic::Status> {
        let (op, container, blob, _) = parse_dispatch(request_json)?;
        if op != "get" {
            return Err(tonic::Status::invalid_argument(format!(
                "get_object expects op=\"get\", got '{op}'"
            )));
        }
        let blob_client = self.client.container(&container).blob_client(blob.clone());
        // collect() pulls the full blob into memory; large blobs would
        // need streaming — same trade-off as the S3 executor.
        let data = blob_client
            .get_content()
            .await
            .map_err(|e| tonic::Status::internal(format!("azure blob get failed: {e}")))?;
        Ok(data)
    }

    async fn put_object(
        &self,
        request_json: &str,
        bytes: Vec<u8>,
    ) -> Result<String, tonic::Status> {
        let (op, container, blob, content_type) = parse_dispatch(request_json)?;
        if op != "put" {
            return Err(tonic::Status::invalid_argument(format!(
                "put_object expects op=\"put\", got '{op}'"
            )));
        }
        reject_oversized_object(bytes.len())?;
        let blob_client = self.client.container(&container).blob_client(blob.clone());
        let mut put = blob_client.put_block_blob(bytes);
        if let Some(ct) = content_type {
            put = put.content_type(ct);
        }
        put.await
            .map_err(|e| tonic::Status::internal(format!("azure blob put failed: {e}")))?;
        Ok(serde_json::json!({ "ok": true, "container": container, "blob": blob }).to_string())
    }

    async fn delete_object(&self, request_json: &str) -> Result<(), tonic::Status> {
        let (_op, container, blob, _) = parse_dispatch(request_json)?;
        let blob_client = self.client.container(&container).blob_client(blob);
        blob_client
            .delete()
            .await
            .map(|_| ())
            .map_err(|e| tonic::Status::internal(format!("azure blob delete failed: {e}")))
    }
}

impl ResourceAdminExecutor for AzureBlobExecutor {
    async fn ensure_resource(
        &self,
        resource_name: &str,
        _spec_json: &str,
    ) -> Result<(), tonic::Status> {
        // Create container (idempotent — Azure returns 409 if exists,
        // which we translate to Ok).
        let container = self.client.container(resource_name);
        match container.create().await {
            Ok(_) => Ok(()),
            Err(e) if e.to_string().contains("ContainerAlreadyExists") => Ok(()),
            Err(e) => Err(tonic::Status::internal(format!(
                "azure blob create container failed: {e}"
            ))),
        }
    }
    async fn drop_resource(&self, resource_name: &str) -> Result<(), tonic::Status> {
        self.client
            .container(resource_name)
            .delete()
            .await
            .map_err(|e| tonic::Status::internal(format!("azure blob drop container: {e}")))?;
        Ok(())
    }
    async fn list_resources(&self) -> Result<Vec<String>, tonic::Status> {
        use futures::StreamExt;
        let mut out = Vec::new();
        let mut stream = self.client.inner.list_containers().into_stream();
        while let Some(page) = stream.next().await {
            let page =
                page.map_err(|e| tonic::Status::internal(format!("azure blob list: {e}")))?;
            for c in page.containers {
                out.push(c.name);
            }
        }
        Ok(out)
    }
}

impl BackendExecutor for AzureBlobExecutor {
    async fn transaction(&self, _: &str) -> Result<String, tonic::Status> {
        Err(tonic::Status::failed_precondition(
            "UDB_UNSUPPORTED_OPERATION: Azure Blob has no transaction primitive",
        ))
    }
    async fn probe(&self) -> Result<BackendProbe, tonic::Status> {
        Ok(build_probe("azureblob", self.ping().await))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_dispatch_extracts_op_container_blob() {
        let req = r#"{"op":"get","container":"docs","blob":"a.pdf"}"#;
        let (op, container, blob, _) = parse_dispatch(req).unwrap();
        assert_eq!(op, "get");
        assert_eq!(container, "docs");
        assert_eq!(blob, "a.pdf");
    }

    #[test]
    fn parse_dispatch_accepts_bucket_key_aliases() {
        let req = r#"{"op":"put","bucket":"docs","key":"a.pdf","content_type":"application/pdf"}"#;
        let (_, container, blob, ct) = parse_dispatch(req).unwrap();
        assert_eq!(container, "docs");
        assert_eq!(blob, "a.pdf");
        assert_eq!(ct.as_deref(), Some("application/pdf"));
    }

    #[test]
    fn parse_dispatch_rejects_missing_container() {
        let req = r#"{"op":"get","blob":"x"}"#;
        let err = parse_dispatch(req).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }
}
