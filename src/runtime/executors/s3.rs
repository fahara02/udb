//! S3 / MinIO object-store executor.
#![allow(clippy::result_large_err)]
//!
//! `BackendExecutor` over `aws_sdk_s3::Client` (newtype to satisfy the orphan
//! rule and to keep trait methods off the external client type). Stateless leaf
//! I/O; instance selection / circuit-breaking stay in the orchestration layer.
//! Request shapes mirror the former inline arms in `core.rs`.

use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use serde_json::Value as JsonValue;

use crate::runtime::executor_utils::{
    json_required_str, object_bytes_from_json, reject_oversized_object,
};
use crate::runtime::executors::{
    BackendExecutor, BackendHealth, BackendProbe, MutationExecutor, ObjectExecutor, QueryExecutor,
    ResourceAdminExecutor, SearchExecutor,
};

pub(crate) struct S3Executor(pub(crate) Client);

impl crate::runtime::backend_context::BackendContextEnforcer for S3Executor {
    fn backend_label(&self) -> &str {
        "s3"
    }

    fn enforce(
        &self,
        ctx: &crate::runtime::backend_context::AppliedContext,
    ) -> crate::runtime::backend_context::ContextEffect {
        if ctx.is_empty() {
            return crate::runtime::backend_context::ContextEffect::Advisory {
                recorded_in: "no_context_to_apply".into(),
            };
        }
        // C7/C8: the S3 IR compiler now prepends `t:<tenant>/p:<project>/`
        // to every resolved object key in compile_read/write/delete.
        // Combined with the bucket = schema convention, an S3 PUT or
        // GET cannot escape its tenant namespace at the key layer.
        crate::runtime::backend_context::ContextEffect::Enforced {
            mechanism: "key prefix t:<tenant>/p:<project>/ prepended by compile_read/write/delete"
                .into(),
        }
    }
}

impl BackendHealth for S3Executor {
    async fn ping(&self) -> Result<(), String> {
        // Constructed only from a configured client; reachability is checked by
        // the dedicated probe RPC, so the dispatch ping is a presence check.
        Ok(())
    }
}

impl QueryExecutor for S3Executor {
    async fn query(&self, _request_json: &str) -> Result<String, tonic::Status> {
        Err(tonic::Status::failed_precondition(
            "s3 does not support generic query",
        ))
    }
}

impl MutationExecutor for S3Executor {
    async fn mutate(&self, _request_json: &str) -> Result<String, tonic::Status> {
        Err(tonic::Status::failed_precondition(
            "s3 mutation is via get_object/put_object",
        ))
    }
}

impl SearchExecutor for S3Executor {
    async fn search(&self, _request_json: &str) -> Result<String, tonic::Status> {
        Err(tonic::Status::failed_precondition(
            "s3 does not support search",
        ))
    }
}

impl ObjectExecutor for S3Executor {
    /// `{"bucket":"b","object_key"|"key":"k"}`.
    async fn get_object(&self, request_json: &str) -> Result<Vec<u8>, tonic::Status> {
        let spec: JsonValue = serde_json::from_str(request_json)
            .map_err(|e| tonic::Status::invalid_argument(format!("invalid request json: {e}")))?;
        let bucket = json_required_str(&spec, "bucket")?;
        let object_key =
            json_required_str(&spec, "object_key").or_else(|_| json_required_str(&spec, "key"))?;
        let output = self
            .0
            .get_object()
            .bucket(bucket)
            .key(object_key)
            .send()
            .await
            .map_err(|err| tonic::Status::unavailable(format!("S3 get_object failed: {err}")))?;
        let bytes = output
            .body
            .collect()
            .await
            .map_err(|err| tonic::Status::unavailable(format!("S3 body read failed: {err}")))?
            .into_bytes()
            .to_vec();
        Ok(bytes)
    }

    /// `{"bucket","object_key"|"key", "content_type"?, inline bytes...}`.
    async fn put_object(
        &self,
        request_json: &str,
        bytes: Vec<u8>,
    ) -> Result<String, tonic::Status> {
        let spec: JsonValue = serde_json::from_str(request_json)
            .map_err(|e| tonic::Status::invalid_argument(format!("invalid request json: {e}")))?;
        let bucket = json_required_str(&spec, "bucket")?;
        let object_key =
            json_required_str(&spec, "object_key").or_else(|_| json_required_str(&spec, "key"))?;
        let body = if bytes.is_empty() {
            object_bytes_from_json(&spec)?
        } else {
            bytes
        };
        // Capture the length before moving `body` into the stream, so the body is
        // not cloned just to report its size in the response (#102).
        let body_len = body.len();
        reject_oversized_object(body_len)?;
        let mut request = self
            .0
            .put_object()
            .bucket(bucket)
            .key(object_key)
            .body(ByteStream::from(body));
        if let Some(content_type) = spec
            .get("content_type")
            .and_then(JsonValue::as_str)
            .filter(|value| !value.trim().is_empty())
        {
            request = request.content_type(content_type);
        }
        request
            .send()
            .await
            .map_err(|err| tonic::Status::unavailable(format!("S3 put_object failed: {err}")))?;
        Ok(serde_json::json!({
            "resource_uri": format!("s3://{bucket}/{object_key}"),
            "affected_rows": 1,
            "bytes": body_len
        })
        .to_string())
    }

    /// `{"bucket","object_key"|"key"}` — deletes the object. S3 delete is
    /// idempotent (deleting a missing key succeeds).
    async fn delete_object(&self, request_json: &str) -> Result<(), tonic::Status> {
        let spec: JsonValue = serde_json::from_str(request_json)
            .map_err(|e| tonic::Status::invalid_argument(format!("invalid request json: {e}")))?;
        let bucket = json_required_str(&spec, "bucket")?;
        let object_key =
            json_required_str(&spec, "object_key").or_else(|_| json_required_str(&spec, "key"))?;
        self.0
            .delete_object()
            .bucket(bucket)
            .key(object_key)
            .send()
            .await
            .map(|_| ())
            .map_err(|err| tonic::Status::unavailable(format!("S3 delete_object failed: {err}")))
    }
}

impl ResourceAdminExecutor for S3Executor {
    async fn ensure_resource(
        &self,
        resource_name: &str,
        _spec_json: &str,
    ) -> Result<(), tonic::Status> {
        self.0
            .create_bucket()
            .bucket(resource_name)
            .send()
            .await
            .map(|_| ())
            .or_else(|err| {
                let msg = err.to_string();
                // BucketAlreadyOwnedByYou / BucketAlreadyExists are idempotent success.
                if msg.contains("BucketAlreadyOwnedByYou") || msg.contains("BucketAlreadyExists") {
                    Ok(())
                } else {
                    Err(tonic::Status::internal(format!(
                        "s3 create_bucket failed: {msg}"
                    )))
                }
            })
    }
    async fn drop_resource(&self, resource_name: &str) -> Result<(), tonic::Status> {
        self.0
            .delete_bucket()
            .bucket(resource_name)
            .send()
            .await
            .map(|_| ())
            .map_err(|e| tonic::Status::internal(format!("s3 delete_bucket failed: {e}")))
    }
    async fn list_resources(&self) -> Result<Vec<String>, tonic::Status> {
        let resp = self
            .0
            .list_buckets()
            .send()
            .await
            .map_err(|e| tonic::Status::internal(format!("s3 list_buckets failed: {e}")))?;
        Ok(resp
            .buckets()
            .iter()
            .filter_map(|b| b.name())
            .map(str::to_owned)
            .collect())
    }
}

impl BackendExecutor for S3Executor {
    async fn transaction(&self, _request_json: &str) -> Result<String, tonic::Status> {
        Err(tonic::Status::failed_precondition(
            "s3 does not support transactions",
        ))
    }
    async fn probe(&self) -> Result<BackendProbe, tonic::Status> {
        Ok(BackendProbe {
            backend: "s3".to_string(),
            instance: None,
            ok: true,
            error: None,
        })
    }
}
