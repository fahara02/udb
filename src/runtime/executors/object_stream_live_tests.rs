//! A.6 live object-streaming verification (env-gated, `#[ignore]`).
//!
//! In its own `*_tests.rs` file (excluded from the
//! `runtime_env_reads_are_confined_to_startup_config_or_compat_boundaries`
//! guardrail) because exercising a live object store legitimately reads endpoint
//! / credential env vars at test time.
//!
//! Runs against ANY S3-compatible store, not just MinIO: point the
//! `UDB_LIVE_S3_*` vars at AWS S3 / Cloudflare R2 / Wasabi / Backblaze B2 / etc.
//! Defaults target the local integration MinIO so the test runs out of the box.

use super::s3::S3Executor;
use super::{ExecutorByteStream, ObjectExecutor, ResourceAdminExecutor};

/// Build an `S3Executor` for the configured S3-compatible endpoint. Resolution
/// (each with a MinIO-integration fallback so the test works locally unchanged):
///   * endpoint   — `UDB_LIVE_S3_ENDPOINT` → `UDB_INTEGRATION_MINIO_ENDPOINT`
///                  → `http://127.0.0.1:59000`
///   * access key — `UDB_LIVE_S3_ACCESS_KEY` → `UDB_INTEGRATION_MINIO_ACCESS_KEY`
///                  → `minio`
///   * secret key — `UDB_LIVE_S3_SECRET_KEY` → `UDB_INTEGRATION_MINIO_SECRET_KEY`
///                  → `minio123`
///   * region     — `UDB_LIVE_S3_REGION` → `us-east-1`
///   * path style — `UDB_LIVE_S3_PATH_STYLE` (default on for MinIO; set `0`/`false`
///                  for virtual-hosted AWS S3)
#[cfg(feature = "s3")]
fn s3_executor_from_env() -> S3Executor {
    use aws_sdk_s3::config::{Credentials, Region};

    fn var_chain(keys: &[&str], default: &str) -> String {
        for key in keys {
            if let Ok(value) = std::env::var(key) {
                if !value.trim().is_empty() {
                    return value;
                }
            }
        }
        default.to_string()
    }

    let endpoint = var_chain(
        &["UDB_LIVE_S3_ENDPOINT", "UDB_INTEGRATION_MINIO_ENDPOINT"],
        "http://127.0.0.1:59000",
    );
    let access = var_chain(
        &["UDB_LIVE_S3_ACCESS_KEY", "UDB_INTEGRATION_MINIO_ACCESS_KEY"],
        "minio",
    );
    let secret = var_chain(
        &["UDB_LIVE_S3_SECRET_KEY", "UDB_INTEGRATION_MINIO_SECRET_KEY"],
        "minio123",
    );
    let region = var_chain(&["UDB_LIVE_S3_REGION"], "us-east-1");
    let path_style = !matches!(
        std::env::var("UDB_LIVE_S3_PATH_STYLE")
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str(),
        "0" | "false" | "no" | "off"
    );

    let creds = Credentials::new(access, secret, None, None, "udb-a6-test");
    let conf = aws_sdk_s3::Config::builder()
        .behavior_version(aws_config::BehaviorVersion::latest())
        .credentials_provider(creds)
        .region(Region::new(region))
        .endpoint_url(endpoint)
        .force_path_style(path_style)
        .build();
    S3Executor(aws_sdk_s3::Client::from_conf(conf))
}

/// Deterministic pseudo-random payload of `len` bytes.
fn payload_of(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

/// `ExecutorByteStream` over `payload` framed in `chunk`-byte gRPC-style chunks.
#[cfg(feature = "s3")]
fn byte_stream(payload: &[u8], chunk: usize) -> ExecutorByteStream {
    let chunks: Vec<Result<bytes::Bytes, tonic::Status>> = payload
        .chunks(chunk.max(1))
        .map(|c| Ok(bytes::Bytes::copy_from_slice(c)))
        .collect();
    Box::pin(tokio_stream::iter(chunks))
}

/// A.6 round-trip across EVERY `put_object_stream` code path against a live
/// S3-compatible store (default MinIO; override with `UDB_LIVE_S3_*` to target
/// AWS S3 / R2 / Wasabi / B2): empty, tiny, sub-part-size, exact-part-size, and
/// large multi-part objects all upload (multipart with abort-on-error, or the
/// single-`PutObject` fast path for sub-part objects) and download (chunked)
/// byte-for-byte without the runtime buffering the whole object. Gated on
/// `UDB_LIVE_OBJECT_TESTS=1`; cleans up the bucket + objects it creates.
#[tokio::test]
#[ignore = "requires a live S3-compatible store: set UDB_LIVE_OBJECT_TESTS=1"]
async fn s3_object_stream_roundtrip_all_sizes_live() {
    use tokio_stream::StreamExt as _;

    if std::env::var("UDB_LIVE_OBJECT_TESTS").is_err() {
        eprintln!("skipping: set UDB_LIVE_OBJECT_TESTS=1 to run against a live S3 store");
        return;
    }
    let executor = s3_executor_from_env();
    let bucket = format!("udb-a6-{}", uuid::Uuid::new_v4().simple());
    executor
        .ensure_resource(&bucket, "{}")
        .await
        .expect("create bucket");

    const MIB: usize = 1024 * 1024;
    // Covers: empty body, single-PutObject fast path (< 8 MiB part), the exact
    // part-size boundary (one full multipart part, empty remainder), and a
    // multi-part upload with a sub-part-size trailing remainder (8+8+4).
    let cases: &[(&str, usize)] = &[
        ("empty", 0),
        ("tiny-1k", 1024),
        ("sub-part-5mib", 5 * MIB),
        ("exact-part-8mib", 8 * MIB),
        ("multipart-20mib", 20 * MIB),
    ];

    for (label, len) in cases {
        let key = format!("a6/{label}.bin");
        let payload = payload_of(*len);
        let request_json = serde_json::json!({ "bucket": bucket, "object_key": key }).to_string();

        executor
            .put_object_stream(&request_json, byte_stream(&payload, MIB))
            .await
            .unwrap_or_else(|e| panic!("[{label}] streaming upload failed: {e}"));

        let mut download = executor
            .get_object_stream(&request_json)
            .await
            .unwrap_or_else(|e| panic!("[{label}] streaming download failed: {e}"));
        let mut got: Vec<u8> = Vec::with_capacity(*len);
        while let Some(chunk) = download.next().await {
            got.extend_from_slice(
                &chunk.unwrap_or_else(|e| panic!("[{label}] download chunk error: {e}")),
            );
        }

        assert_eq!(got.len(), payload.len(), "[{label}] round-trip byte count");
        assert!(got == payload, "[{label}] round-trip content matches");

        let _ = executor.delete_object(&request_json).await;
    }

    let _ = executor.drop_resource(&bucket).await;
}
