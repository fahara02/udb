//! OBJ1/2/3 — LIVE tenant-isolation of the SERVED data-plane OBJECT path.
//!
//! Reproduces the customer-reported cross-tenant object leak against a REAL
//! S3/MinIO. Every served object entrypoint physically namespaces the object key
//! by the VERIFIED-claim tenant (`__udb_t/<tenant>/<key>`) via
//! `tenant_scoped_object_key` (src/runtime/executor_utils.rs), so two tenants
//! that PUT/GET/presign the SAME `bucket`+`key` can never read, overwrite, or
//! mint a URL for each other's object.
//!
//! Served entrypoints exercised (all on `DataBrokerRuntime`, the impls the gRPC
//! `GeneratePresignedUrl` / `GetObject` / multipart handlers delegate to):
//!   * `generate_presigned_url`  (PUT + GET)
//!   * `get_object`              (streamed read-back)
//!   * `initiate_multipart_upload`
//!
//! The write half of the customer symptom is driven through a tenant-A presigned
//! PUT URL (a served entrypoint that applies the SAME `tenant_scoped_object_key`)
//! + a real HTTP upload — landing the bytes exactly where the streaming
//! `PutObject` would — and the read half through the served `get_object`.
//!
//! Reverting `tenant_scoped_object_key` (returning the raw key) makes each
//! assertion below fail: tenant B reads tenant A's bytes, and a tenant-A URL no
//! longer carries A's namespaced key.
//!
//! Run with a live MinIO + Postgres (see the session runbook):
//!   UDB_LIVE_OBJECT_TESTS=1 UDB_MINIO_ENDPOINT=http://127.0.0.1:19000 \
//!     cargo test --lib object_ -- --ignored --nocapture

use super::support::live_runtime;
use crate::generation::{CatalogManifest, ManifestStore, ManifestStoreOption};
use crate::runtime::core::setup_data::object_request_json;
use uuid::Uuid;

/// Point the runtime's MinIO instance at the live integration store. Mirrors the
/// env the storage-object round-trip live tests set before `live_runtime()`.
fn configure_minio_env() {
    unsafe {
        std::env::set_var(
            "UDB_MINIO_ENDPOINT",
            std::env::var("UDB_MINIO_ENDPOINT")
                .unwrap_or_else(|_| "http://127.0.0.1:19000".to_string()),
        );
        std::env::set_var(
            "UDB_MINIO_ACCESS_KEY",
            std::env::var("UDB_MINIO_ACCESS_KEY").unwrap_or_else(|_| "udbminio".to_string()),
        );
        std::env::set_var(
            "UDB_MINIO_SECRET_KEY",
            std::env::var("UDB_MINIO_SECRET_KEY").unwrap_or_else(|_| "udbminio123".to_string()),
        );
        std::env::set_var(
            "UDB_MINIO_REGION",
            std::env::var("UDB_MINIO_REGION").unwrap_or_else(|_| "us-east-1".to_string()),
        );
        std::env::set_var("UDB_ALLOW_DEGRADED_BACKENDS", "true");
    }
}

fn storage_bucket() -> String {
    std::env::var("UDB_STORAGE_BUCKET").unwrap_or_else(|_| "udb-storage".to_string())
}

/// Manifest carrying ONE presign-enabled object store bound to `bucket`. The
/// served object path resolves the store by `resource_name == bucket`; the
/// physical tenant prefix is applied downstream and is not declared here.
fn object_store_manifest(bucket: &str) -> CatalogManifest {
    CatalogManifest {
        checksum_sha256: format!("obj-iso-{bucket}"),
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

/// Verified-claim context for `tenant`, carrying the object scopes the served
/// path requires (`udb:object:presign` for presign, `udb:stream` for get).
fn object_context(tenant: &str) -> crate::RequestContext {
    crate::RequestContext {
        tenant_id: tenant.to_string(),
        project_id: "default".to_string(),
        purpose: "object-iso-test".to_string(),
        scopes: vec!["udb:object:presign".to_string(), "udb:stream".to_string()],
        ..Default::default()
    }
}

/// Read a served `get_object` stream to completion, returning the object bytes.
async fn read_object(
    runtime: &crate::runtime::DataBrokerRuntime,
    manifest: &CatalogManifest,
    bucket: &str,
    key: &str,
    ctx: crate::RequestContext,
) -> Result<Vec<u8>, tonic::Status> {
    use tokio_stream::StreamExt as _;
    let mut stream = runtime
        .get_object(
            manifest,
            crate::proto::ObjectRequest {
                context: None,
                bucket: bucket.to_string(),
                object_key: key.to_string(),
            },
            ctx,
        )
        .await?;
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        bytes.extend_from_slice(chunk?.data.as_ref());
    }
    Ok(bytes)
}

/// OBJ1/2/3 (presign + multipart): a tenant-A presigned URL must target A's
/// namespaced physical key, and a tenant-B URL must target B's — never each
/// other's — for the SAME bucket+key. Revert of `tenant_scoped_object_key` ⇒ the
/// URLs carry the raw key and the `__udb_t/<tenant>/` assertions fail.
#[tokio::test]
#[ignore = "requires live MinIO; run with UDB_LIVE_OBJECT_TESTS=1 ... -- --ignored"]
async fn object_presign_and_multipart_are_tenant_namespaced_live() {
    configure_minio_env();
    let runtime = live_runtime().await;
    let bucket = storage_bucket();
    let manifest = object_store_manifest(&bucket);
    let key = format!("iso/{}.txt", Uuid::new_v4().simple());
    let tenant_a = format!("tnt-a-{}", Uuid::new_v4().simple());
    let tenant_b = format!("tnt-b-{}", Uuid::new_v4().simple());

    // Tenant A mints a presigned PUT for (bucket, key).
    let put_a = runtime
        .generate_presigned_url(
            &manifest,
            crate::proto::UrlRequest {
                context: None,
                bucket: bucket.clone(),
                object_key: key.clone(),
                method: "PUT".to_string(),
                ttl_seconds: 300,
                content_type: "text/plain".to_string(),
            },
            object_context(&tenant_a),
        )
        .await
        .expect("tenant-A presigned PUT");
    assert!(
        put_a.url.contains("__udb_t") && put_a.url.contains(&tenant_a),
        "tenant-A presigned PUT must target A's namespaced key; got {}",
        put_a.url
    );

    // Tenant B mints a presigned GET for the SAME (bucket, key). It must target
    // B's namespaced key — never A's.
    let get_b = runtime
        .generate_presigned_url(
            &manifest,
            crate::proto::UrlRequest {
                context: None,
                bucket: bucket.clone(),
                object_key: key.clone(),
                method: "GET".to_string(),
                ttl_seconds: 300,
                content_type: String::new(),
            },
            object_context(&tenant_b),
        )
        .await
        .expect("tenant-B presigned GET");
    assert!(
        get_b.url.contains(&tenant_b),
        "tenant-B presigned GET must target B's namespaced key; got {}",
        get_b.url
    );
    assert!(
        !get_b.url.contains(&tenant_a),
        "tenant-B presigned GET must NOT target tenant-A's key (cross-tenant leak); got {}",
        get_b.url
    );

    // Multipart initiation: every part URL must target A's namespaced key.
    let multipart = runtime
        .initiate_multipart_upload(
            &manifest,
            crate::proto::MultipartUploadRequest {
                context: None,
                bucket: bucket.clone(),
                object_key: key.clone(),
                content_type: "text/plain".to_string(),
                part_count: 1,
                ttl_seconds: 300,
                idempotency_key: String::new(),
            },
            object_context(&tenant_a),
        )
        .await
        .expect("tenant-A multipart init");
    let part = multipart
        .part_urls
        .first()
        .expect("multipart must return at least one part URL");
    assert!(
        part.contains("__udb_t") && part.contains(&tenant_a),
        "multipart part URL must target A's namespaced key; got {part}"
    );

    // Best-effort: abort the multipart upload's object namespace isn't cleaned
    // here (no bytes were uploaded); the created upload expires on its own.
}

/// OBJ1/2/3 (the headline symptom): tenant A writes bytes to (bucket, key);
/// tenant B reading (bucket, key) MUST NOT get A's bytes, while tenant A can.
/// Revert of `tenant_scoped_object_key` ⇒ both tenants share the raw key and B
/// reads A's bytes.
#[tokio::test]
#[ignore = "requires live MinIO; run with UDB_LIVE_OBJECT_TESTS=1 ... -- --ignored"]
async fn object_put_then_cross_tenant_get_is_isolated_live() {
    configure_minio_env();
    let runtime = live_runtime().await;
    let bucket = storage_bucket();
    let manifest = object_store_manifest(&bucket);
    let key = format!("iso/{}.txt", Uuid::new_v4().simple());
    let tenant_a = format!("tnt-a-{}", Uuid::new_v4().simple());
    let tenant_b = format!("tnt-b-{}", Uuid::new_v4().simple());
    let payload = b"tenant-A-secret-object-bytes".to_vec();

    // Tenant A writes via a served presigned PUT (applies tenant_scoped_object_key),
    // then a real HTTP upload lands the bytes at A's namespaced physical key.
    let put_a = runtime
        .generate_presigned_url(
            &manifest,
            crate::proto::UrlRequest {
                context: None,
                bucket: bucket.clone(),
                object_key: key.clone(),
                method: "PUT".to_string(),
                ttl_seconds: 300,
                content_type: "text/plain".to_string(),
            },
            object_context(&tenant_a),
        )
        .await
        .expect("tenant-A presigned PUT");
    let http = reqwest::Client::new();
    let resp = http
        .put(&put_a.url)
        .header("content-type", "text/plain")
        .body(payload.clone())
        .send()
        .await
        .expect("HTTP PUT to tenant-A presigned URL");
    assert!(
        resp.status().is_success(),
        "tenant-A presigned upload failed: {}",
        resp.status()
    );

    // Tenant A reads (bucket, key) back through the served get_object → its bytes.
    let a_bytes = read_object(
        &runtime,
        &manifest,
        &bucket,
        &key,
        object_context(&tenant_a),
    )
    .await
    .expect("tenant-A served get_object");
    assert_eq!(
        a_bytes, payload,
        "tenant A must read back its own object bytes"
    );

    // Tenant B reads the SAME (bucket, key) → must NOT see A's bytes. A miss
    // surfaces either as an error or as a non-matching body; both are acceptable,
    // returning A's exact bytes is the leak.
    // A miss on B's namespaced key surfaces as an error (the correct isolated
    // outcome); only a SUCCESSFUL read by B must not equal A's bytes.
    if let Ok(b_bytes) = read_object(
        &runtime,
        &manifest,
        &bucket,
        &key,
        object_context(&tenant_b),
    )
    .await
    {
        assert_ne!(
            b_bytes, payload,
            "CROSS-TENANT LEAK: tenant B read tenant A's object bytes"
        );
    }

    // Cleanup: best-effort remove A's namespaced physical object.
    let physical_key = format!("__udb_t/{tenant_a}/{key}");
    let _ = runtime
        .delete_object_backend_target(
            "minio",
            None,
            "default",
            &object_request_json("delete", &bucket, &physical_key, ""),
        )
        .await;
}
