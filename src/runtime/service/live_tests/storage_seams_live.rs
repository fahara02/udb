//! Ledger section F (cache / object / storage) seam tests, live on the CI
//! backends (Postgres, MinIO, Redis). Each test drives the SERVED path (the
//! storage/cache service RPC or the runtime entrypoint the gRPC handler
//! delegates to) and then reads the durable state back — a Postgres row, a
//! MinIO HEAD, or a Redis-backed stats call — so a regression in either the
//! user-visible answer or the persisted outcome fails the test.
//!
//! Run in the CI live lane:
//!   UDB_LIVE_AUTH_TESTS=1 UDB_LIVE_OBJECT_TESTS=1 cargo test --lib \
//!     storage_seams_live -- --ignored --test-threads=1 --nocapture

use super::support::*;
use crate::proto::udb::core::storage::services::v1 as storage_pb;
use crate::proto::udb::core::storage::services::v1::storage_service_server::StorageService;
use crate::runtime::service::storage_service::StorageServiceImpl;
use tonic::{Code, Request};
use uuid::Uuid;

/// Point the runtime's MinIO instance at the live integration store (the CI
/// live-lane defaults when the env is not already set). Mirrors the other
/// object live tests.
fn configure_minio_env() {
    unsafe {
        std::env::set_var(
            "UDB_MINIO_ENDPOINT",
            std::env::var("UDB_MINIO_ENDPOINT")
                .unwrap_or_else(|_| "http://127.0.0.1:59000".to_string()),
        );
        std::env::set_var(
            "UDB_MINIO_ACCESS_KEY",
            std::env::var("UDB_MINIO_ACCESS_KEY").unwrap_or_else(|_| "minio".to_string()),
        );
        std::env::set_var(
            "UDB_MINIO_SECRET_KEY",
            std::env::var("UDB_MINIO_SECRET_KEY").unwrap_or_else(|_| "minio123".to_string()),
        );
        std::env::set_var(
            "UDB_MINIO_REGION",
            std::env::var("UDB_MINIO_REGION").unwrap_or_else(|_| "us-east-1".to_string()),
        );
        std::env::set_var("UDB_ALLOW_DEGRADED_BACKENDS", "true");
    }
}

/// Scoped env override: the knobs under test (`UDB_MAX_OBJECT_BYTES`,
/// `UDB_STORAGE_TENANT_QUOTA_BYTES`) are read on every call, not cached, so the
/// served path sees the override immediately. The previous value is restored on
/// drop (the live lane runs `--test-threads=1`, so no other test observes it).
struct EnvOverride {
    key: &'static str,
    previous: Option<String>,
}

impl EnvOverride {
    fn set(key: &'static str, value: &str) -> Self {
        let previous = std::env::var(key).ok();
        unsafe {
            std::env::set_var(key, value);
        }
        Self { key, previous }
    }
}

impl Drop for EnvOverride {
    fn drop(&mut self) {
        unsafe {
            match self.previous.as_deref() {
                Some(value) => std::env::set_var(self.key, value),
                None => std::env::remove_var(self.key),
            }
        }
    }
}

fn tenant_request<T>(message: T, tenant_id: &str) -> Request<T> {
    let mut request = Request::new(message);
    request.metadata_mut().insert(
        "x-tenant-id",
        tenant_id.parse().expect("valid tenant metadata"),
    );
    request
}

/// The `error-reason` trailer the storage service stamps on non-OK statuses.
fn error_reason(status: &tonic::Status) -> Option<String> {
    status
        .metadata()
        .get("error-reason")
        .and_then(|value| value.to_str().ok())
        .map(str::to_string)
}

/// Decode the typed `ErrorDetail` trailer every refusal carries.
#[cfg_attr(not(feature = "s3"), allow(dead_code))]
fn error_detail(status: &tonic::Status) -> crate::proto::ErrorDetail {
    let raw = status
        .metadata()
        .get_bin(crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY)
        .expect("typed ErrorDetail trailer is present");
    crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
}

/// Manifest carrying ONE presign-enabled MinIO object store bound to `bucket`
/// (the served data-plane object path resolves the store by bucket name).
#[cfg(feature = "s3")]
fn object_store_manifest(bucket: &str) -> crate::generation::CatalogManifest {
    use crate::generation::{CatalogManifest, ManifestStore, ManifestStoreOption};
    CatalogManifest {
        checksum_sha256: format!("storage-seams-{bucket}"),
        stores: vec![ManifestStore {
            store_kind: "object".to_string(),
            backend: "minio".to_string(),
            resource_name: bucket.to_string(),
            options: vec![
                ManifestStoreOption {
                    key: "presigned_read".to_string(),
                    value: "true".to_string(),
                },
                ManifestStoreOption {
                    key: "presigned_write".to_string(),
                    value: "true".to_string(),
                },
            ],
            ..ManifestStore::default()
        }],
        ..CatalogManifest::default()
    }
}

/// Verified-claim context for `tenant` with the data-plane object scopes.
#[cfg(feature = "s3")]
fn object_context(tenant: &str) -> crate::RequestContext {
    crate::RequestContext {
        tenant_id: tenant.to_string(),
        project_id: "default".to_string(),
        purpose: "storage-seams-test".to_string(),
        scopes: vec!["udb:object:presign".to_string(), "udb:stream".to_string()],
        ..Default::default()
    }
}

/// F1: a multipart upload whose uploaded parts total more than
/// `UDB_MAX_OBJECT_BYTES` is refused at CompleteMultipartUpload with
/// RESOURCE_EXHAUSTED (typed QUOTA detail), and the upload is aborted: the
/// store no longer knows the upload id and no object was assembled.
#[cfg(feature = "s3")]
#[tokio::test]
#[ignore = "requires live MinIO+Postgres; run with UDB_LIVE_OBJECT_TESTS=1 ... -- --ignored --test-threads=1"]
async fn f1_multipart_complete_over_object_cap_is_resource_exhausted_and_aborted() {
    configure_minio_env();
    let runtime = live_runtime().await;
    let bucket = std::env::var("UDB_STORAGE_BUCKET").unwrap_or_else(|_| "udb-storage".to_string());
    let manifest = object_store_manifest(&bucket);
    let key = format!("seams-f1/{}.bin", Uuid::new_v4().simple());
    let tenant = format!("tnt-f1-{}", Uuid::new_v4().simple());
    let payload = vec![b'x'; 64];

    let upload = runtime
        .initiate_multipart_upload(
            &manifest,
            crate::proto::MultipartUploadRequest {
                context: None,
                bucket: bucket.clone(),
                object_key: key.clone(),
                content_type: "application/octet-stream".to_string(),
                part_count: 1,
                ttl_seconds: 300,
                idempotency_key: String::new(),
            },
            object_context(&tenant),
        )
        .await
        .expect("multipart init");
    let resp = reqwest::Client::new()
        .put(&upload.part_urls[0])
        .body(payload.clone())
        .send()
        .await
        .expect("HTTP PUT to the part URL");
    assert!(resp.status().is_success(), "part upload: {}", resp.status());
    let etag = resp
        .headers()
        .get("etag")
        .and_then(|v| v.to_str().ok())
        .expect("part upload returns an ETag")
        .to_string();

    // The ceiling is read per call; set it below the uploaded part size.
    let refused = {
        let _cap = EnvOverride::set("UDB_MAX_OBJECT_BYTES", "16");
        runtime
            .complete_multipart_upload(
                &manifest,
                crate::proto::CompleteMultipartUploadRequest {
                    context: None,
                    bucket: bucket.clone(),
                    object_key: key.clone(),
                    upload_id: upload.upload_id.clone(),
                    parts: vec![crate::proto::MultipartUploadPart {
                        part_number: 1,
                        etag: etag.clone(),
                    }],
                    idempotency_key: String::new(),
                },
                object_context(&tenant),
            )
            .await
            .expect_err("an over-ceiling multipart completion must be refused")
    };
    assert_eq!(refused.code(), Code::ResourceExhausted, "{refused}");
    assert!(
        refused.message().contains("UDB_MAX_OBJECT_BYTES"),
        "refusal must name the ceiling: {}",
        refused.message()
    );
    assert_eq!(
        error_detail(&refused).kind,
        crate::proto::ErrorKind::Quota as i32,
        "refusal must carry a typed QUOTA detail"
    );

    // Read back: the upload was aborted. With the ceiling lifted, completing
    // the same upload again can no longer list its parts — while it existed
    // the same call answered ResourceExhausted. (A second Abort proves nothing:
    // S3/MinIO abort is idempotent and reports success either way.)
    let retried = runtime
        .complete_multipart_upload(
            &manifest,
            crate::proto::CompleteMultipartUploadRequest {
                context: None,
                bucket: bucket.clone(),
                object_key: key.clone(),
                upload_id: upload.upload_id.clone(),
                parts: vec![crate::proto::MultipartUploadPart {
                    part_number: 1,
                    etag: etag.clone(),
                }],
                idempotency_key: String::new(),
            },
            object_context(&tenant),
        )
        .await
        .expect_err("an aborted upload can no longer be completed");
    assert_ne!(
        retried.code(),
        Code::ResourceExhausted,
        "the parts must be gone, not still over the ceiling: {retried}"
    );
    // ... and no object was assembled at the tenant-namespaced physical key.
    let physical_key =
        crate::runtime::executor_utils::tenant_scoped_object_key(&object_context(&tenant), &key);
    let head = runtime
        .object_exists_backend_target("minio", "", &bucket, &physical_key)
        .await
        .expect("HEAD the would-be object");
    assert!(
        head.is_none(),
        "no object may exist after an over-ceiling completion: {head:?}"
    );
}

/// F2: the orphan reaper hard-deletes a stale PENDING file but never an ACTIVE
/// one, even when both are older than the orphan cutoff.
#[tokio::test]
#[ignore = "requires live Postgres+MinIO; run with UDB_LIVE_AUTH_TESTS=1 ... -- --ignored --test-threads=1"]
async fn f2_orphan_reaper_keeps_active_file_and_reaps_stale_pending() {
    let _guard = live_native_service_db_lock().lock().await;
    configure_minio_env();
    let pool = live_pg_pool().await;
    migrate_native_service_db(&pool).await;
    let svc = storage_service(pool.clone()).await;
    let tenant_id = Uuid::new_v4().to_string();

    let active_id = seed_storage_file(&pool, &tenant_id).await;
    let pending_id = seed_storage_file(&pool, &tenant_id).await;
    // Make the ACTIVE file look exactly like an orphan except for its status:
    // both rows are backdated well past the cutoff.
    sqlx::query(
        "UPDATE udb_storage.files SET status = 'ACTIVE', created_at = NOW() - INTERVAL '3 days' \
         WHERE file_id::text = $1",
    )
    .bind(&active_id)
    .execute(&pool)
    .await
    .expect("mark seeded file ACTIVE + backdate");
    sqlx::query(
        "UPDATE udb_storage.files SET created_at = NOW() - INTERVAL '3 days' \
         WHERE file_id::text = $1",
    )
    .bind(&pending_id)
    .execute(&pool)
    .await
    .expect("backdate stale PENDING file");

    svc.reap_orphans(60, 500).await.expect("orphan reaper pass");

    let status_of = |file_id: String| {
        let pool = pool.clone();
        async move {
            sqlx::query_scalar::<_, String>(
                "SELECT status FROM udb_storage.files WHERE file_id::text = $1",
            )
            .bind(file_id)
            .fetch_optional(&pool)
            .await
            .expect("read back file row")
        }
    };
    assert_eq!(
        status_of(active_id.clone()).await.as_deref(),
        Some("ACTIVE"),
        "the reaper must never delete an ACTIVE file"
    );
    assert_eq!(
        status_of(pending_id.clone()).await,
        None,
        "a stale PENDING orphan must be hard-deleted"
    );

    cleanup_native_service_db(&pool).await;
}

/// F3: a SOFT DeleteFile whose byte delete fails (the file's bucket does not
/// exist) still tombstones the metadata, reports OBJECT_DELETE_FAILED on the
/// response, and leaves a durable SOFT GC intent for the sweep.
#[tokio::test]
#[ignore = "requires live Postgres+MinIO; run with UDB_LIVE_AUTH_TESTS=1 ... -- --ignored --test-threads=1"]
async fn f3_soft_delete_missing_bucket_reports_object_delete_failed_and_records_gc_intent() {
    let _guard = live_native_service_db_lock().lock().await;
    configure_minio_env();
    let pool = live_pg_pool().await;
    migrate_native_service_db(&pool).await;
    let missing_bucket = format!("udb-missing-{}", Uuid::new_v4().simple());
    // Same wiring as `build_storage_service`, but the service owns its bytes in
    // a bucket that does not exist, so every byte delete fails.
    let svc = StorageServiceImpl::new()
        .with_postgres(Some(pool.clone()))
        .with_object(
            Some(live_runtime().await),
            "minio".to_string(),
            missing_bucket.clone(),
        );
    let tenant_id = Uuid::new_v4().to_string();

    let reg = svc
        .register_upload(Request::new(storage_pb::RegisterUploadRequest {
            tenant_id: tenant_id.clone(),
            filename: "orphan.txt".to_string(),
            content_type: "text/plain".to_string(),
            file_type: "DOCUMENT".to_string(),
            ..Default::default()
        }))
        .await
        .expect("register_upload")
        .into_inner();

    let deleted = svc
        .delete_file(Request::new(storage_pb::DeleteFileRequest {
            tenant_id: tenant_id.clone(),
            file_id: reg.file_id.clone(),
            reason: "f3 seam".to_string(),
            ..Default::default()
        }))
        .await
        .expect("soft delete stays OK; the byte failure rides the response")
        .into_inner();
    assert!(deleted.success, "metadata tombstone is committed");
    let error = deleted
        .error
        .expect("a failed byte delete must be reported, never swallowed");
    assert_eq!(error.code, "OBJECT_DELETE_FAILED", "{}", error.message);

    // Read back: the metadata is tombstoned ...
    let (status, deleted_at_set): (String, bool) = sqlx::query_as(
        "SELECT status, deleted_at IS NOT NULL FROM udb_storage.files WHERE file_id::text = $1",
    )
    .bind(&reg.file_id)
    .fetch_one(&pool)
    .await
    .expect("read back tombstoned file row");
    assert_eq!(status, "DELETED");
    assert!(deleted_at_set, "soft delete must stamp deleted_at");
    // ... and a durable SOFT GC intent targets the unreachable bytes.
    let intents: Vec<(String, String, String, String)> = sqlx::query_as(
        "SELECT mode, status, bucket, object_key FROM udb_storage.gc_intents \
         WHERE tenant_id = $1::uuid AND file_id = $2::uuid",
    )
    .bind(&tenant_id)
    .bind(&reg.file_id)
    .fetch_all(&pool)
    .await
    .expect("read back GC intents");
    assert_eq!(intents.len(), 1, "exactly one GC intent: {intents:?}");
    let (mode, intent_status, bucket, object_key) = &intents[0];
    assert_eq!(mode, "SOFT");
    assert_eq!(intent_status, "PENDING");
    assert_eq!(bucket, &missing_bucket);
    assert_eq!(object_key, &reg.object_key);

    cleanup_native_service_db(&pool).await;
}

/// F4: bytes uploaded through the public data-plane object path (the documented
/// fallback when no native upload URL is available) land at the SAME physical
/// key the storage finalize/download/delete candidate list probes, so
/// FinalizeUpload finds them and the file becomes ACTIVE.
#[cfg(feature = "s3")]
#[tokio::test]
#[ignore = "requires live Postgres+MinIO; run with UDB_LIVE_OBJECT_TESTS=1 ... -- --ignored --test-threads=1"]
async fn f4_put_object_fallback_key_matches_finalize_key_scheme() {
    let _guard = live_native_service_db_lock().lock().await;
    configure_minio_env();
    let pool = live_pg_pool().await;
    migrate_native_service_db(&pool).await;
    let svc = storage_service(pool.clone()).await;
    let runtime = live_runtime().await;
    let bucket = svc.object_bucket.clone();
    let manifest = object_store_manifest(&bucket);
    let tenant_id = Uuid::new_v4().to_string();
    let payload = b"f4-fallback-bytes".to_vec();

    let reg = svc
        .register_upload(Request::new(storage_pb::RegisterUploadRequest {
            tenant_id: tenant_id.clone(),
            filename: "fallback.txt".to_string(),
            content_type: "text/plain".to_string(),
            file_type: "DOCUMENT".to_string(),
            size_bytes: payload.len() as i64,
            ..Default::default()
        }))
        .await
        .expect("register_upload")
        .into_inner();

    // The fallback key is the data-plane tenant-namespaced key, and it is the
    // second candidate finalize probes (native presign key first).
    let fallback_key = crate::runtime::executor_utils::tenant_scoped_object_key(
        &object_context(&tenant_id),
        &reg.object_key,
    );
    assert_eq!(
        crate::runtime::service::storage_service::file_object_key_candidates(
            &tenant_id,
            &reg.object_key
        ),
        vec![reg.object_key.clone(), fallback_key.clone()],
        "finalize must probe the native key and the PutObject fallback key"
    );

    // Upload through the served data-plane object path (it applies the same
    // tenant namespacing the streaming PutObject applies).
    let put = runtime
        .generate_presigned_url(
            &manifest,
            crate::proto::UrlRequest {
                context: None,
                bucket: bucket.clone(),
                object_key: reg.object_key.clone(),
                method: "PUT".to_string(),
                ttl_seconds: 300,
                content_type: "text/plain".to_string(),
            },
            object_context(&tenant_id),
        )
        .await
        .expect("data-plane presigned PUT");
    let resp = reqwest::Client::new()
        .put(&put.url)
        .header("content-type", "text/plain")
        .body(payload.clone())
        .send()
        .await
        .expect("HTTP PUT via the data-plane URL");
    assert!(
        resp.status().is_success(),
        "fallback PUT: {}",
        resp.status()
    );

    // Read back in MinIO: bytes exist at the fallback key, not the bare key.
    let at_fallback = runtime
        .object_exists_backend_target("minio", "", &bucket, &fallback_key)
        .await
        .expect("HEAD fallback key");
    assert_eq!(
        at_fallback.map(|(size, _)| size),
        Some(payload.len() as i64),
        "fallback upload must land at the tenant-namespaced key"
    );
    let at_bare = runtime
        .object_exists_backend_target("minio", "", &bucket, &reg.object_key)
        .await
        .expect("HEAD bare key");
    assert!(
        at_bare.is_none(),
        "fallback upload must not use the bare key"
    );

    // Served finalize finds the fallback bytes and activates the file.
    let file = svc
        .finalize_upload(Request::new(storage_pb::FinalizeUploadRequest {
            tenant_id: tenant_id.clone(),
            file_id: reg.file_id.clone(),
            content_type: "text/plain".to_string(),
            size_bytes: payload.len() as i64,
            ..Default::default()
        }))
        .await
        .expect("finalize must find bytes uploaded via the PutObject fallback")
        .into_inner()
        .file
        .expect("finalized file");
    assert_eq!(
        file.status,
        crate::proto::udb::core::storage::entity::v1::FileStatus::Active as i32
    );
    let (status, size): (String, i64) = sqlx::query_as(
        "SELECT status, size_bytes::bigint FROM udb_storage.files WHERE file_id::text = $1",
    )
    .bind(&reg.file_id)
    .fetch_one(&pool)
    .await
    .expect("read back finalized row");
    assert_eq!(status, "ACTIVE");
    assert_eq!(size, payload.len() as i64);

    let _ = runtime
        .delete_object_backend_target(
            "minio",
            None,
            "default",
            &crate::runtime::core::setup_data::object_request_json(
                "delete",
                &bucket,
                &fallback_key,
                "",
            ),
        )
        .await;
    cleanup_native_service_db(&pool).await;
}

/// F5: under `UDB_STORAGE_TENANT_QUOTA_BYTES` the declared size of a PENDING
/// registration IS a quota reservation made before its upload URL is issued: a
/// registration beyond the remaining quota is refused (RESOURCE_EXHAUSTED,
/// STORAGE_QUOTA_EXCEEDED) without a row or URL, an undeclared size is refused,
/// and a registration that exactly fills the quota is admitted.
#[tokio::test]
#[ignore = "requires live Postgres+MinIO; run with UDB_LIVE_AUTH_TESTS=1 ... -- --ignored --test-threads=1"]
async fn f5_upload_url_beyond_remaining_quota_is_refused_and_reservation_counted() {
    let _guard = live_native_service_db_lock().lock().await;
    configure_minio_env();
    let pool = live_pg_pool().await;
    migrate_native_service_db(&pool).await;
    let svc = storage_service(pool.clone()).await;
    let tenant_id = Uuid::new_v4().to_string();
    let _quota = EnvOverride::set("UDB_STORAGE_TENANT_QUOTA_BYTES", "100");

    let register = |size_bytes: i64, filename: &'static str| {
        let svc = &svc;
        let tenant_id = tenant_id.clone();
        async move {
            svc.register_upload(tenant_request(
                storage_pb::RegisterUploadRequest {
                    tenant_id: tenant_id.clone(),
                    filename: filename.to_string(),
                    content_type: "text/plain".to_string(),
                    file_type: "DOCUMENT".to_string(),
                    size_bytes,
                    ..Default::default()
                },
                &tenant_id,
            ))
            .await
        }
    };

    let first = register(60, "first.txt")
        .await
        .expect("a registration within quota is admitted")
        .into_inner();
    assert!(
        !first.upload_url.is_empty(),
        "an admitted reservation is issued an upload URL: {:?}",
        first.error
    );

    let over = register(50, "over.txt")
        .await
        .expect_err("60 reserved + 50 > 100 must be refused before a URL is issued");
    assert_eq!(over.code(), Code::ResourceExhausted, "{over}");
    assert_eq!(
        error_reason(&over).as_deref(),
        Some("STORAGE_QUOTA_EXCEEDED")
    );

    let undeclared = register(0, "undeclared.txt")
        .await
        .expect_err("an undeclared size reserves nothing and must be refused");
    assert_eq!(undeclared.code(), Code::InvalidArgument, "{undeclared}");

    let fill = register(40, "fill.txt")
        .await
        .expect("a registration exactly filling the quota is admitted")
        .into_inner();
    assert!(!fill.upload_url.is_empty(), "{:?}", fill.error);

    // Read back: only the two admitted PENDING reservations exist, and their
    // declared sizes are what the quota counts.
    let (rows, reserved): (i64, i64) = sqlx::query_as(
        "SELECT COUNT(*)::bigint, COALESCE(SUM(size_bytes), 0)::bigint FROM udb_storage.files \
         WHERE tenant_id::text = $1 AND status = 'PENDING' AND deleted_at IS NULL",
    )
    .bind(&tenant_id)
    .fetch_one(&pool)
    .await
    .expect("read back quota reservations");
    assert_eq!(rows, 2, "refused registrations must not leave a row");
    assert_eq!(reserved, 100, "PENDING declared sizes are the reservation");
    assert_eq!(
        svc.tenant_scoped_size_sum(&tenant_id)
            .await
            .expect("quota usage aggregate"),
        100,
        "the quota gate counts PENDING reservations"
    );

    cleanup_native_service_db(&pool).await;
}

/// F6: the native cache namespace byte budget is enforced atomically
/// (server-side Lua): 20 concurrent 100-byte Sets into a 1000-byte namespace
/// admit exactly 10 and refuse 10 with RESOURCE_EXHAUSTED, and the namespace
/// stats read back exactly the budget.
#[cfg(feature = "redis")]
#[tokio::test]
#[ignore = "requires live Redis; run with UDB_INTEGRATION_REDIS_URL=redis://127.0.0.1:56379 ... -- --ignored --test-threads=1"]
async fn f6_cache_byte_budget_admits_exactly_budget_under_concurrent_sets() {
    use crate::proto::udb::core::cache::services::v1 as cache_pb;
    use crate::proto::udb::core::cache::services::v1::cache_service_server::CacheService;
    use crate::runtime::service::cache_service::CacheServiceImpl;
    use std::sync::Arc;

    let redis_url = super::support::live_env("UDB_INTEGRATION_REDIS_URL")
        .or_else(|| super::support::live_env("UDB_REDIS_DSN"))
        .unwrap_or_else(|| "redis://127.0.0.1:56379".to_string());
    let client = redis::Client::open(redis_url.as_str()).expect("create live redis client");
    let svc = Arc::new(CacheServiceImpl::new().with_redis(Some(client)));
    let tenant_id = Uuid::new_v4().to_string();
    let namespace = format!("f6-{}", Uuid::new_v4().simple());

    eprintln!("F6 cache budget: creating namespace");
    svc.create_namespace(tenant_request(
        cache_pb::CreateNamespaceRequest {
            tenant_id: tenant_id.clone(),
            namespace: namespace.clone(),
            max_bytes: 1000,
            ..Default::default()
        },
        &tenant_id,
    ))
    .await
    .expect("create budgeted namespace");

    eprintln!("F6 cache budget: running 20 concurrent sets");
    let mut tasks = Vec::new();
    for i in 0..20 {
        let svc = svc.clone();
        let tenant_id = tenant_id.clone();
        let namespace = namespace.clone();
        tasks.push(tokio::spawn(async move {
            svc.set(tenant_request(
                cache_pb::SetRequest {
                    tenant_id: tenant_id.clone(),
                    namespace,
                    key: format!("k{i}"),
                    value: vec![b'v'; 100],
                    ttl_seconds: 0,
                },
                &tenant_id,
            ))
            .await
        }));
    }
    let mut stored = 0;
    let mut refused = 0;
    for task in tasks {
        match task.await.expect("set task joins") {
            Ok(_) => stored += 1,
            Err(status) if status.code() == Code::ResourceExhausted => refused += 1,
            Err(status) => panic!("unexpected cache Set failure: {status}"),
        }
    }
    assert_eq!(stored, 10, "exactly budget/size Sets may be admitted");
    assert_eq!(refused, 10, "every Set past the budget is refused");

    eprintln!("F6 cache budget: reading namespace stats");
    let stats = svc
        .get_namespace_stats(tenant_request(
            cache_pb::GetNamespaceStatsRequest {
                tenant_id: tenant_id.clone(),
                namespace: namespace.clone(),
            },
            &tenant_id,
        ))
        .await
        .expect("namespace stats")
        .into_inner();
    assert_eq!(stats.used_bytes, 1000, "budget counter never overshoots");
    assert_eq!(stats.max_bytes, 1000);
    assert_eq!(stats.item_count, 10, "only admitted entries were written");
}

/// F7: presign and HEAD are S3/MinIO-only; dispatching either at Azure Blob or
/// GCS fails with an explicit FAILED_PRECONDITION capability error naming the
/// backend and the required capability, before any network call (so no live
/// backend is needed).
#[cfg(feature = "s3")]
#[tokio::test]
async fn f7_azureblob_presign_and_gcs_head_dispatch_fail_with_capability_error() {
    let runtime = crate::runtime::DataBrokerRuntime::default();

    let presign = runtime
        .presign_object_backend_target(
            "azureblob",
            "default",
            "udb-storage",
            "f7/object.txt",
            "PUT",
            "text/plain",
            300,
        )
        .await
        .expect_err("Azure Blob presign must be refused explicitly");
    let head = runtime
        .object_exists_backend_target("gcs", "default", "udb-storage", "f7/object.txt")
        .await
        .expect_err("GCS HEAD must be refused explicitly");

    for (status, backend, operation) in [
        (&presign, "azureblob", "presign_object_backend_target"),
        (&head, "gcs", "object_exists_backend_target"),
    ] {
        assert_eq!(status.code(), Code::FailedPrecondition, "{status}");
        assert!(
            status.message().contains(backend) && status.message().contains("S3/MinIO only"),
            "message must name the backend and the capability: {}",
            status.message()
        );
        let detail = error_detail(status);
        assert_eq!(detail.kind, crate::proto::ErrorKind::Capability as i32);
        assert_eq!(detail.backend, backend);
        assert_eq!(detail.operation, operation);
        assert_eq!(detail.capability_required, "s3_compatible_object_store");
    }
}
