//! service.rs split — object RPC handlers (Phase G).
use super::*;

impl DataBrokerService {
    pub(crate) async fn put_object_inner(
        &self,
        request: Request<tonic::Streaming<Chunk>>,
    ) -> Result<Response<MutationResponse>, Status> {
        let started = Instant::now();
        let security = match security_from_request(&request) {
            Ok(s) => s,
            Err(e) => return self.record_grpc("PutObject", started, Err(e)),
        };
        if let Err(err) = self.authorize(&security, "*", "PutObject").await {
            return self.record_grpc("PutObject", started, Err(err));
        }
        let manifest = &self.catalog.active_for(&security.project_id).manifest;
        let runtime = self.runtime_snapshot();
        let metadata_context = security.request_context();
        let execution_context = metadata_context.clone();
        let response_context = metadata_context.clone();
        let result = self
            .execute_with_channel_scoped(
                crate::runtime::channels::OperationChannel::Object,
                Some(&metadata_context),
                Some("s3"),
                || async move {
                    runtime
                        .put_object(manifest, request.into_inner(), execution_context)
                        .await
                },
            )
            .await;

        match result {
            Ok(res) => self.record_grpc(
                "PutObject",
                started,
                Ok(self
                    .with_mutation_response_headers(res, &response_context)
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
        let response_context = metadata_context.clone();
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
            Ok(chunks) => self.record_grpc(
                "GetObject",
                started,
                Ok(self.with_catalog_response_headers(
                    Response::new(Box::pin(tokio_stream::iter(chunks.into_iter().map(Ok)))
                        as ResponseStream<Chunk>),
                    &response_context,
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
        let response_context = metadata_context.clone();
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
                Ok(self.with_catalog_response_headers(Response::new(res), &response_context)),
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
        let response_context = metadata_context.clone();
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
                Ok(self.with_catalog_response_headers(Response::new(res), &response_context)),
            ),
            Err(err) => self.record_grpc("InitiateMultipartUpload", started, Err(err)),
        }
    }
}
