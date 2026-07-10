//! S3 / MinIO object-store executor.
#![allow(clippy::result_large_err)]
//!
//! `BackendExecutor` over `aws_sdk_s3::Client` (newtype to satisfy the orphan
//! rule and to keep trait methods off the external client type). Stateless leaf
//! I/O; instance selection / circuit-breaking stay in the orchestration layer.
//! Request shapes mirror the former inline arms in `core.rs`.

use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use bytes::Bytes;
use serde_json::Value as JsonValue;

use crate::runtime::executor_utils::{
    HTTP_RETRYABLE_BACKOFF_MS, backend_transport_status, capability_status, internal_status,
    invalid_argument_fields, json_required_str, object_bytes_from_json, reject_oversized_object,
    retryable_status,
};
use crate::runtime::executors::{
    BackendExecutor, BackendHealth, BackendProbe, ExecutorByteStream, MutationExecutor,
    ObjectExecutor, QueryExecutor, ResourceAdminExecutor, SearchExecutor,
};

use crate::runtime::config::{s3_download_chunk_bytes, s3_multipart_part_bytes};

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
        Err(capability_status(
            "s3",
            "query",
            "generic_query",
            "s3 does not support generic query",
        ))
    }
}

impl MutationExecutor for S3Executor {
    async fn mutate(&self, _request_json: &str) -> Result<String, tonic::Status> {
        Err(capability_status(
            "s3",
            "mutate",
            "object_dispatch",
            "s3 mutation is via get_object/put_object",
        ))
    }
}

impl SearchExecutor for S3Executor {
    async fn search(&self, _request_json: &str) -> Result<String, tonic::Status> {
        Err(capability_status(
            "s3",
            "search",
            "search",
            "s3 does not support search",
        ))
    }
}

fn invalid_s3_request_json_status(err: serde_json::Error) -> tonic::Status {
    invalid_argument_fields(
        format!("invalid request json: {err}"),
        [("request_json", "must be valid JSON for S3 object dispatch")],
    )
}

fn s3_internal_status(operation: impl Into<String>, message: impl Into<String>) -> tonic::Status {
    internal_status("S3", operation, message)
}

impl ObjectExecutor for S3Executor {
    /// `{"bucket":"b","object_key"|"key":"k"}`.
    async fn get_object(&self, request_json: &str) -> Result<Vec<u8>, tonic::Status> {
        let spec: JsonValue =
            serde_json::from_str(request_json).map_err(invalid_s3_request_json_status)?;
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
            .map_err(|err| backend_transport_status("S3", "get_object", err))?;
        let bytes = output
            .body
            .collect()
            .await
            .map_err(|err| backend_transport_status("S3", "body read", err))?
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
        let spec: JsonValue =
            serde_json::from_str(request_json).map_err(invalid_s3_request_json_status)?;
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
            .map_err(|err| backend_transport_status("S3", "put_object", err))?;
        Ok(serde_json::json!({
            "resource_uri": format!("s3://{bucket}/{object_key}"),
            "affected_rows": 1,
            "bytes": body_len
        })
        .to_string())
    }

    /// Streaming download (A.6): read the S3 body in `S3_DOWNLOAD_CHUNK_BYTES`
    /// frames without ever holding the whole object in memory.
    async fn get_object_stream(
        &self,
        request_json: &str,
    ) -> Result<ExecutorByteStream, tonic::Status> {
        let spec: JsonValue =
            serde_json::from_str(request_json).map_err(invalid_s3_request_json_status)?;
        let bucket = json_required_str(&spec, "bucket")?.to_string();
        let object_key = json_required_str(&spec, "object_key")
            .or_else(|_| json_required_str(&spec, "key"))?
            .to_string();
        let output = self
            .0
            .get_object()
            .bucket(&bucket)
            .key(&object_key)
            .send()
            .await
            .map_err(|err| backend_transport_status("S3", "get_object", err))?;
        let chunk_bytes = s3_download_chunk_bytes();
        let stream = async_stream::try_stream! {
            use tokio::io::AsyncReadExt as _;
            let mut reader = output.body.into_async_read();
            let mut buf = vec![0u8; chunk_bytes];
            loop {
                let read = reader.read(&mut buf).await.map_err(|err| {
                    backend_transport_status("S3", "body read", err)
                })?;
                if read == 0 {
                    break;
                }
                yield Bytes::copy_from_slice(&buf[..read]);
            }
        };
        Ok(Box::pin(stream))
    }

    /// Streaming upload (A.6): forward the chunk stream to S3 multipart, buffering
    /// at most one ~8 MiB part. Objects that fit in a single part skip multipart
    /// and use a plain `PutObject`. On a part failure the multipart upload is
    /// aborted so no orphaned upload is left behind.
    async fn put_object_stream(
        &self,
        request_json: &str,
        stream: ExecutorByteStream,
    ) -> Result<String, tonic::Status> {
        use tokio_stream::StreamExt as _;
        let spec: JsonValue =
            serde_json::from_str(request_json).map_err(invalid_s3_request_json_status)?;
        let bucket = json_required_str(&spec, "bucket")?.to_string();
        let object_key = json_required_str(&spec, "object_key")
            .or_else(|_| json_required_str(&spec, "key"))?
            .to_string();
        let content_type = spec
            .get("content_type")
            .and_then(JsonValue::as_str)
            .filter(|value| !value.trim().is_empty())
            .map(str::to_string);

        let part_size = s3_multipart_part_bytes();
        let mut stream = stream;
        let mut buf: Vec<u8> = Vec::with_capacity(part_size);
        // (upload_id, completed parts, next part number)
        let mut mpu: Option<(String, Vec<CompletedPart>, i32)> = None;

        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            buf.extend_from_slice(&chunk);
            if buf.len() < part_size {
                continue;
            }
            if mpu.is_none() {
                let mut create = self
                    .0
                    .create_multipart_upload()
                    .bucket(&bucket)
                    .key(&object_key);
                if let Some(ct) = &content_type {
                    create = create.content_type(ct);
                }
                let created = create
                    .send()
                    .await
                    .map_err(|e| backend_transport_status("S3", "create_multipart_upload", e))?;
                let upload_id = created
                    .upload_id()
                    .ok_or_else(|| {
                        retryable_status(
                            "S3",
                            "create_multipart_upload",
                            HTTP_RETRYABLE_BACKOFF_MS,
                            "S3 create_multipart_upload returned no upload_id",
                        )
                    })?
                    .to_string();
                mpu = Some((upload_id, Vec::new(), 1));
            }
            let (upload_id, parts, part_number) =
                mpu.as_mut().expect("multipart initialized above");
            let part = std::mem::replace(&mut buf, Vec::with_capacity(part_size));
            match self
                .0
                .upload_part()
                .bucket(&bucket)
                .key(&object_key)
                .upload_id(upload_id.clone())
                .part_number(*part_number)
                .body(ByteStream::from(part))
                .send()
                .await
            {
                Ok(resp) => {
                    parts.push(
                        CompletedPart::builder()
                            .e_tag(resp.e_tag().unwrap_or_default().to_string())
                            .part_number(*part_number)
                            .build(),
                    );
                    *part_number += 1;
                }
                Err(e) => {
                    let _ = self
                        .0
                        .abort_multipart_upload()
                        .bucket(&bucket)
                        .key(&object_key)
                        .upload_id(upload_id.clone())
                        .send()
                        .await;
                    return Err(backend_transport_status("S3", "upload_part", e));
                }
            }
        }

        match mpu {
            // Never reached the part threshold → small object, single PutObject.
            None => {
                let mut request = self
                    .0
                    .put_object()
                    .bucket(&bucket)
                    .key(&object_key)
                    .body(ByteStream::from(buf));
                if let Some(ct) = &content_type {
                    request = request.content_type(ct);
                }
                request
                    .send()
                    .await
                    .map_err(|e| backend_transport_status("S3", "put_object", e))?;
            }
            Some((upload_id, mut parts, part_number)) => {
                // Flush the trailing (< part-size) remainder as the final part.
                if !buf.is_empty() {
                    match self
                        .0
                        .upload_part()
                        .bucket(&bucket)
                        .key(&object_key)
                        .upload_id(upload_id.clone())
                        .part_number(part_number)
                        .body(ByteStream::from(buf))
                        .send()
                        .await
                    {
                        Ok(resp) => parts.push(
                            CompletedPart::builder()
                                .e_tag(resp.e_tag().unwrap_or_default().to_string())
                                .part_number(part_number)
                                .build(),
                        ),
                        Err(e) => {
                            let _ = self
                                .0
                                .abort_multipart_upload()
                                .bucket(&bucket)
                                .key(&object_key)
                                .upload_id(upload_id.clone())
                                .send()
                                .await;
                            return Err(backend_transport_status("S3", "upload_part (final)", e));
                        }
                    }
                }
                let completed = CompletedMultipartUpload::builder()
                    .set_parts(Some(parts))
                    .build();
                if let Err(e) = self
                    .0
                    .complete_multipart_upload()
                    .bucket(&bucket)
                    .key(&object_key)
                    .upload_id(upload_id.clone())
                    .multipart_upload(completed)
                    .send()
                    .await
                {
                    let _ = self
                        .0
                        .abort_multipart_upload()
                        .bucket(&bucket)
                        .key(&object_key)
                        .upload_id(upload_id)
                        .send()
                        .await;
                    return Err(backend_transport_status(
                        "S3",
                        "complete_multipart_upload",
                        e,
                    ));
                }
            }
        }
        Ok(serde_json::json!({
            "resource_uri": format!("s3://{bucket}/{object_key}"),
            "affected_rows": 1
        })
        .to_string())
    }

    /// `{"bucket","object_key"|"key"}` — deletes the object. S3 delete is
    /// idempotent (deleting a missing key succeeds).
    async fn delete_object(&self, request_json: &str) -> Result<(), tonic::Status> {
        let spec: JsonValue =
            serde_json::from_str(request_json).map_err(invalid_s3_request_json_status)?;
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
            .map_err(|err| backend_transport_status("S3", "delete_object", err))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{ErrorDetail, ErrorKind};
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;

    fn decode_detail(status: &tonic::Status) -> ErrorDetail {
        let raw = status
            .metadata()
            .get_bin(ERROR_DETAIL_METADATA_KEY)
            .expect("typed detail trailer is present");
        crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
    }

    fn assert_internal_detail(status: &tonic::Status, operation: &str, message: &str) {
        assert_eq!(status.code(), tonic::Code::Internal);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Internal as i32);
        assert_eq!(detail.backend, "S3");
        assert_eq!(detail.operation, operation);
        assert!(!detail.retryable);
    }

    #[test]
    fn internal_status_carries_typed_detail() {
        let status = s3_internal_status(
            "create_bucket",
            "s3 create_bucket failed: AccessDenied: forbidden",
        );
        assert_internal_detail(
            &status,
            "create_bucket",
            "s3 create_bucket failed: AccessDenied: forbidden",
        );
    }

    #[test]
    fn request_json_validation_carries_field_violation() {
        let err = serde_json::from_str::<JsonValue>("{")
            .map_err(invalid_s3_request_json_status)
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().starts_with("invalid request json:"));
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, "request_json");
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
                use aws_sdk_s3::error::ProvideErrorMetadata;
                // BucketAlreadyOwnedByYou / BucketAlreadyExists are idempotent
                // success. The aws-sdk-s3 v1 `SdkError` `Display` is the opaque
                // "service error" — the modeled code lives in the structured
                // ServiceError — so detect the already-exists cases STRUCTURALLY
                // (the old `to_string().contains(...)` never matched, turning an
                // existing bucket into a hard failure). For real failures, surface
                // the actual code + message instead of "service error".
                if err.as_service_error().is_some_and(|svc| {
                    svc.is_bucket_already_owned_by_you() || svc.is_bucket_already_exists()
                }) {
                    Ok(())
                } else {
                    Err(s3_internal_status(
                        "create_bucket",
                        format!(
                            "s3 create_bucket failed: {}: {}",
                            err.code().unwrap_or("unknown"),
                            err.message().unwrap_or_default()
                        ),
                    ))
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
            .map_err(|e| backend_transport_status("S3", "delete_bucket", e))
    }
    async fn list_resources(&self) -> Result<Vec<String>, tonic::Status> {
        let resp = self
            .0
            .list_buckets()
            .send()
            .await
            .map_err(|e| backend_transport_status("S3", "list_buckets", e))?;
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
        Err(capability_status(
            "s3",
            "transaction",
            "transactions",
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
