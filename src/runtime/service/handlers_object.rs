//! service.rs split — object RPC handlers (Phase G).
use super::*;

impl DataBrokerService {
    pub(crate) async fn put_object_inner(
        &self,
        request: Request<tonic::Streaming<Chunk>>,
    ) -> Result<Response<MutationResponse>, Status> {
        let (started, security) = authorized_call!(self, request, "PutObject");
        // The control gate above carries no resource. Read the FIRST chunk (it
        // names the bucket) and authorize the write against that real bucket,
        // exactly like GetObject / GeneratePresignedUrl / multipart, BEFORE any
        // byte is forwarded to the store.
        let mut stream = request.into_inner();
        let first = match tokio_stream::StreamExt::next(&mut stream).await {
            Some(Ok(chunk)) => chunk,
            Some(Err(err)) => return self.record_grpc("PutObject", started, Err(err)),
            None => {
                return self.record_grpc(
                    "PutObject",
                    started,
                    Err(crate::runtime::executor_utils::invalid_argument_fields(
                        "empty object stream",
                        [(
                            "stream",
                            "object upload stream must contain at least one chunk",
                        )],
                    )),
                );
            }
        };
        if let Err(err) = self.authorize(&security, &first.bucket, "PutObject").await {
            return self.record_grpc("PutObject", started, Err(err));
        }
        let manifest = &self.catalog.active_for(&security.project_id).manifest;
        let runtime = self.runtime_snapshot();
        let metadata_context = security.request_context();
        let execution_context = metadata_context.clone();
        let result = self
            .execute_with_channel_scoped(
                crate::runtime::channels::OperationChannel::Object,
                Some(&metadata_context),
                Some("s3"),
                || async move {
                    runtime
                        .put_object_with_first(manifest, first, stream, execution_context)
                        .await
                },
            )
            .await;

        match result {
            Ok(res) => self.record_grpc(
                "PutObject",
                started,
                Ok(self
                    .with_mutation_response_headers(res, &metadata_context)
                    .await),
            ),
            Err(err) => self.record_grpc("PutObject", started, Err(err)),
        }
    }

    pub(crate) async fn get_object_inner(
        &self,
        request: Request<crate::proto::ObjectRequest>,
    ) -> Result<Response<ResponseStream<Chunk>>, Status> {
        let started = Instant::now();
        let security = match security_from_request(&request) {
            Ok(s) => s,
            Err(e) => return self.record_grpc("GetObject", started, Err(e)),
        };
        let request = request.into_inner();
        if let Err(err) = self
            .authorize(&security, &request.bucket, "GetObject")
            .await
        {
            return self.record_grpc("GetObject", started, Err(err));
        }
        self.metrics.inc_object_op(&request.bucket, "GET");
        let manifest = &self.catalog.active_for(&security.project_id).manifest;
        let runtime = self.runtime_snapshot();
        let metadata_context = security.request_context();
        let execution_context = metadata_context.clone();
        let result = self
            .execute_with_channel_scoped(
                crate::runtime::channels::OperationChannel::Object,
                Some(&metadata_context),
                Some("s3"),
                || async move {
                    runtime
                        .get_object(manifest, request, execution_context)
                        .await
                },
            )
            .await;

        match result {
            Ok(stream) => self.record_grpc(
                "GetObject",
                started,
                Ok(self.with_catalog_response_headers(
                    Response::new(stream as ResponseStream<Chunk>),
                    &metadata_context,
                )),
            ),
            Err(err) => self.record_grpc("GetObject", started, Err(err)),
        }
    }

    pub(crate) async fn generate_presigned_url_inner(
        &self,
        request: Request<UrlRequest>,
    ) -> Result<Response<UrlResponse>, Status> {
        let started = Instant::now();
        let security = match security_from_request(&request) {
            Ok(s) => s,
            Err(e) => return self.record_grpc("GeneratePresignedUrl", started, Err(e)),
        };
        let request = request.into_inner();
        if let Err(err) = self
            .authorize(&security, &request.bucket, "GeneratePresignedUrl")
            .await
        {
            return self.record_grpc("GeneratePresignedUrl", started, Err(err));
        }
        self.metrics.inc_object_op(&request.bucket, &request.method);
        let manifest = &self.catalog.active_for(&security.project_id).manifest;
        let runtime = self.runtime_snapshot();
        let metadata_context = security.request_context();
        let execution_context = metadata_context.clone();
        let result = self
            .execute_with_channel_scoped(
                crate::runtime::channels::OperationChannel::Object,
                Some(&metadata_context),
                Some("s3"),
                || async move {
                    runtime
                        .generate_presigned_url(manifest, request, execution_context)
                        .await
                },
            )
            .await;

        match result {
            Ok(res) => self.record_grpc(
                "GeneratePresignedUrl",
                started,
                Ok(self.with_catalog_response_headers(Response::new(res), &metadata_context)),
            ),
            Err(err) => self.record_grpc("GeneratePresignedUrl", started, Err(err)),
        }
    }

    pub(crate) async fn initiate_multipart_upload_inner(
        &self,
        request: Request<MultipartUploadRequest>,
    ) -> Result<Response<MultipartUploadResponse>, Status> {
        let started = Instant::now();
        let security = match security_from_request(&request) {
            Ok(s) => s,
            Err(e) => return self.record_grpc("InitiateMultipartUpload", started, Err(e)),
        };
        let request = request.into_inner();
        if let Err(err) = self
            .authorize(&security, &request.bucket, "InitiateMultipartUpload")
            .await
        {
            return self.record_grpc("InitiateMultipartUpload", started, Err(err));
        }
        self.metrics.inc_object_op(&request.bucket, "MULTIPART");
        let manifest = &self.catalog.active_for(&security.project_id).manifest;
        let runtime = self.runtime_snapshot();
        let metadata_context = security.request_context();
        let execution_context = metadata_context.clone();
        let result = self
            .execute_with_channel_scoped(
                crate::runtime::channels::OperationChannel::Object,
                Some(&metadata_context),
                Some("s3"),
                || async move {
                    runtime
                        .initiate_multipart_upload(manifest, request, execution_context)
                        .await
                },
            )
            .await;

        match result {
            Ok(res) => self.record_grpc(
                "InitiateMultipartUpload",
                started,
                Ok(self.with_catalog_response_headers(Response::new(res), &metadata_context)),
            ),
            Err(err) => self.record_grpc("InitiateMultipartUpload", started, Err(err)),
        }
    }

    /// Finish a multipart upload. Same security posture as Initiate: verified
    /// security context, per-bucket authorization under this RPC's own action,
    /// the tenant-scoped Object channel, and a tenant-namespaced physical key in
    /// the runtime, so an `upload_id` can only be completed by its own tenant.
    pub(crate) async fn complete_multipart_upload_inner(
        &self,
        request: Request<crate::proto::CompleteMultipartUploadRequest>,
    ) -> Result<Response<crate::proto::CompleteMultipartUploadResponse>, Status> {
        let started = Instant::now();
        let security = match security_from_request(&request) {
            Ok(s) => s,
            Err(e) => return self.record_grpc("CompleteMultipartUpload", started, Err(e)),
        };
        let request = request.into_inner();
        if let Err(err) = self
            .authorize(&security, &request.bucket, "CompleteMultipartUpload")
            .await
        {
            return self.record_grpc("CompleteMultipartUpload", started, Err(err));
        }
        self.metrics
            .inc_object_op(&request.bucket, "MULTIPART_COMPLETE");
        let manifest = &self.catalog.active_for(&security.project_id).manifest;
        let runtime = self.runtime_snapshot();
        let metadata_context = security.request_context();
        let execution_context = metadata_context.clone();
        let result = self
            .execute_with_channel_scoped(
                crate::runtime::channels::OperationChannel::Object,
                Some(&metadata_context),
                Some("s3"),
                || async move {
                    runtime
                        .complete_multipart_upload(manifest, request, execution_context)
                        .await
                },
            )
            .await;

        match result {
            Ok(res) => self.record_grpc(
                "CompleteMultipartUpload",
                started,
                Ok(self.with_catalog_response_headers(Response::new(res), &metadata_context)),
            ),
            Err(err) => self.record_grpc("CompleteMultipartUpload", started, Err(err)),
        }
    }

    /// Cancel a multipart upload and release its uploaded parts (same posture as
    /// [`Self::complete_multipart_upload_inner`]).
    pub(crate) async fn abort_multipart_upload_inner(
        &self,
        request: Request<crate::proto::AbortMultipartUploadRequest>,
    ) -> Result<Response<crate::proto::AbortMultipartUploadResponse>, Status> {
        let started = Instant::now();
        let security = match security_from_request(&request) {
            Ok(s) => s,
            Err(e) => return self.record_grpc("AbortMultipartUpload", started, Err(e)),
        };
        let request = request.into_inner();
        if let Err(err) = self
            .authorize(&security, &request.bucket, "AbortMultipartUpload")
            .await
        {
            return self.record_grpc("AbortMultipartUpload", started, Err(err));
        }
        self.metrics
            .inc_object_op(&request.bucket, "MULTIPART_ABORT");
        let manifest = &self.catalog.active_for(&security.project_id).manifest;
        let runtime = self.runtime_snapshot();
        let metadata_context = security.request_context();
        let execution_context = metadata_context.clone();
        let result = self
            .execute_with_channel_scoped(
                crate::runtime::channels::OperationChannel::Object,
                Some(&metadata_context),
                Some("s3"),
                || async move {
                    runtime
                        .abort_multipart_upload(manifest, request, execution_context)
                        .await
                },
            )
            .await;

        match result {
            Ok(res) => self.record_grpc(
                "AbortMultipartUpload",
                started,
                Ok(self.with_catalog_response_headers(Response::new(res), &metadata_context)),
            ),
            Err(err) => self.record_grpc("AbortMultipartUpload", started, Err(err)),
        }
    }
}
