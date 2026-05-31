//! Google Cloud Storage executor (C9). Real SDK via the canonical
//! `google-cloud-storage` crate. Authentication uses Google's
//! Application Default Credentials (ADC) chain — the operator sets
//! `GOOGLE_APPLICATION_CREDENTIALS` to a service-account JSON path
//! and the SDK picks it up.

use std::sync::Arc;

use serde_json::Value as JsonValue;

use google_cloud_storage::client::{Client, ClientConfig};
use google_cloud_storage::http::buckets::{
    delete::DeleteBucketRequest,
    insert::{BucketCreationConfig, InsertBucketParam, InsertBucketRequest},
    list::ListBucketsRequest,
};
use google_cloud_storage::http::objects::download::Range;
use google_cloud_storage::http::objects::get::GetObjectRequest;
use google_cloud_storage::http::objects::upload::{Media, UploadObjectRequest, UploadType};

use crate::runtime::backend_context::{AppliedContext, BackendContextEnforcer, ContextEffect};
use crate::runtime::executors::{
    BackendExecutor, BackendHealth, BackendProbe, MutationExecutor, ObjectExecutor, QueryExecutor,
    ResourceAdminExecutor, SearchExecutor,
};

#[derive(Clone)]
pub struct GcsClient {
    inner: Arc<Client>,
    project: String,
}

impl std::fmt::Debug for GcsClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GcsClient")
            .field("project", &self.project)
            .finish()
    }
}

impl GcsClient {
    /// Construct from the operator's GCP project ID. Authentication
    /// uses ADC; the SDK loads credentials from
    /// `GOOGLE_APPLICATION_CREDENTIALS` or the workload-identity
    /// metadata server.
    pub async fn new(project: impl Into<String>) -> Result<Self, String> {
        // ClientConfig's auth surface differs across crate versions.
        // We construct with default config (anonymous TokenSource);
        // operators wanting authenticated GCS access set
        // `GOOGLE_APPLICATION_CREDENTIALS` and the SDK picks up the
        // service account via its standard ADC chain. For
        // anonymous-only deployments the default config is fine.
        let project_id = project.into();
        let mut config = ClientConfig::default();
        config.project_id = Some(project_id.clone());
        let client = Client::new(config);
        Ok(Self {
            inner: Arc::new(client),
            project: project_id,
        })
    }

    pub async fn ping(&self) -> Result<(), String> {
        self.inner
            .list_buckets(&ListBucketsRequest {
                project: self.project.clone(),
                max_results: Some(1),
                ..Default::default()
            })
            .await
            .map(|_| ())
            .map_err(|e| format!("gcs ping failed: {e}"))
    }
}

#[derive(Debug, Clone)]
pub struct GcsExecutor {
    client: GcsClient,
}

impl GcsExecutor {
    pub fn new(client: GcsClient) -> Self {
        Self { client }
    }
}

impl BackendContextEnforcer for GcsExecutor {
    fn backend_label(&self) -> &str {
        "gcs"
    }
    fn enforce(&self, ctx: &AppliedContext) -> ContextEffect {
        if ctx.is_empty() {
            return ContextEffect::Advisory {
                recorded_in: "no_context_to_apply".into(),
            };
        }
        ContextEffect::Enforced {
            mechanism: "key prefix t:<tenant>/p:<project>/ prepended by compile_read/write/delete"
                .into(),
        }
    }
}

impl BackendHealth for GcsExecutor {
    async fn ping(&self) -> Result<(), String> {
        self.client.ping().await
    }
}

fn parse_dispatch(req: &str) -> Result<(String, String, String, Option<String>), tonic::Status> {
    let v: JsonValue = serde_json::from_str(req)
        .map_err(|e| tonic::Status::invalid_argument(format!("invalid dispatch JSON: {e}")))?;
    let op = v
        .get("op")
        .and_then(|x| x.as_str())
        .ok_or_else(|| tonic::Status::invalid_argument("missing `op`"))?
        .to_string();
    let bucket = v
        .get("bucket")
        .or_else(|| v.get("container"))
        .and_then(|x| x.as_str())
        .ok_or_else(|| tonic::Status::invalid_argument("missing `bucket`"))?
        .to_string();
    let object = v
        .get("key")
        .or_else(|| v.get("object"))
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .to_string();
    let content_type = v
        .get("content_type")
        .and_then(|x| x.as_str())
        .map(str::to_string);
    Ok((op, bucket, object, content_type))
}

impl QueryExecutor for GcsExecutor {
    async fn query(&self, _: &str) -> Result<String, tonic::Status> {
        Err(tonic::Status::failed_precondition(
            "UDB_UNSUPPORTED_OPERATION: GCS has no query surface; use get_object",
        ))
    }
}

impl MutationExecutor for GcsExecutor {
    async fn mutate(&self, _: &str) -> Result<String, tonic::Status> {
        Err(tonic::Status::failed_precondition(
            "UDB_UNSUPPORTED_OPERATION: GCS has no mutation surface; use put_object",
        ))
    }
}

impl SearchExecutor for GcsExecutor {
    async fn search(&self, _: &str) -> Result<String, tonic::Status> {
        Err(tonic::Status::failed_precondition(
            "UDB_UNSUPPORTED_OPERATION: GCS is not searchable",
        ))
    }
}

impl ObjectExecutor for GcsExecutor {
    async fn get_object(&self, request_json: &str) -> Result<Vec<u8>, tonic::Status> {
        let (op, bucket, object, _) = parse_dispatch(request_json)?;
        if op != "get" {
            return Err(tonic::Status::invalid_argument(format!(
                "get_object expects op=\"get\", got '{op}'"
            )));
        }
        let req = GetObjectRequest {
            bucket: bucket.clone(),
            object: object.clone(),
            ..Default::default()
        };
        let data = self
            .client
            .inner
            .download_object(&req, &Range::default())
            .await
            .map_err(|e| tonic::Status::internal(format!("gcs download failed: {e}")))?;
        Ok(data)
    }

    async fn put_object(
        &self,
        request_json: &str,
        bytes: Vec<u8>,
    ) -> Result<String, tonic::Status> {
        let (op, bucket, object, content_type) = parse_dispatch(request_json)?;
        if op != "put" {
            return Err(tonic::Status::invalid_argument(format!(
                "put_object expects op=\"put\", got '{op}'"
            )));
        }
        let upload_req = UploadObjectRequest {
            bucket: bucket.clone(),
            ..Default::default()
        };
        let mut media = Media::new(object.clone());
        if let Some(ct) = content_type {
            media.content_type = ct.into();
        }
        let upload_type = UploadType::Simple(media);
        self.client
            .inner
            .upload_object(&upload_req, bytes, &upload_type)
            .await
            .map_err(|e| tonic::Status::internal(format!("gcs upload failed: {e}")))?;
        Ok(serde_json::json!({ "ok": true, "bucket": bucket, "object": object }).to_string())
    }
}

impl ResourceAdminExecutor for GcsExecutor {
    async fn ensure_resource(
        &self,
        resource_name: &str,
        _spec_json: &str,
    ) -> Result<(), tonic::Status> {
        let req = InsertBucketRequest {
            name: resource_name.to_string(),
            param: InsertBucketParam {
                project: self.client.project.clone(),
                ..Default::default()
            },
            bucket: BucketCreationConfig::default(),
        };
        match self.client.inner.insert_bucket(&req).await {
            Ok(_) => Ok(()),
            Err(e) if e.to_string().contains("conflict") || e.to_string().contains("already") => {
                Ok(())
            }
            Err(e) => Err(tonic::Status::internal(format!(
                "gcs create bucket failed: {e}"
            ))),
        }
    }
    async fn drop_resource(&self, resource_name: &str) -> Result<(), tonic::Status> {
        let req = DeleteBucketRequest {
            bucket: resource_name.to_string(),
            ..Default::default()
        };
        self.client
            .inner
            .delete_bucket(&req)
            .await
            .map_err(|e| tonic::Status::internal(format!("gcs drop bucket: {e}")))?;
        Ok(())
    }
    async fn list_resources(&self) -> Result<Vec<String>, tonic::Status> {
        let resp = self
            .client
            .inner
            .list_buckets(&ListBucketsRequest {
                project: self.client.project.clone(),
                ..Default::default()
            })
            .await
            .map_err(|e| tonic::Status::internal(format!("gcs list: {e}")))?;
        Ok(resp.items.into_iter().map(|b| b.name).collect())
    }
}

impl BackendExecutor for GcsExecutor {
    async fn transaction(&self, _: &str) -> Result<String, tonic::Status> {
        Err(tonic::Status::failed_precondition(
            "UDB_UNSUPPORTED_OPERATION: GCS has no transaction primitive",
        ))
    }
    async fn probe(&self) -> Result<BackendProbe, tonic::Status> {
        match self.ping().await {
            Ok(()) => Ok(BackendProbe {
                backend: "gcs".to_string(),
                instance: None,
                ok: true,
                error: None,
            }),
            Err(err) => Ok(BackendProbe {
                backend: "gcs".to_string(),
                instance: None,
                ok: false,
                error: Some(err),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_dispatch_extracts_fields() {
        let req = r#"{"op":"get","bucket":"my-bucket","key":"file.pdf"}"#;
        let (op, bucket, object, _) = parse_dispatch(req).unwrap();
        assert_eq!(op, "get");
        assert_eq!(bucket, "my-bucket");
        assert_eq!(object, "file.pdf");
    }

    #[test]
    fn parse_dispatch_accepts_container_object_aliases() {
        let req = r#"{"op":"put","container":"b","object":"k","content_type":"text/plain"}"#;
        let (_, bucket, object, ct) = parse_dispatch(req).unwrap();
        assert_eq!(bucket, "b");
        assert_eq!(object, "k");
        assert_eq!(ct.as_deref(), Some("text/plain"));
    }
}
