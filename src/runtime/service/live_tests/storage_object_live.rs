//! Live MinIO verification of the object-byte path the storage service uses for
//! `DeleteFile` / orphan-reaper GC (`DataBrokerRuntime::*_object_backend_target`
//! + `object_request_json`). Proves put → get → delete → get-fails end to end
//! against a real object store.
//!
//! Run with a live MinIO + Postgres (see the session runbook):
//!   UDB_LIVE_OBJECT_TESTS=1 cargo test --lib \
//!     live_minio_storage_object_delete_roundtrip -- --ignored --nocapture

use crate::runtime::DataBrokerRuntime;
use crate::runtime::core::setup_data::object_request_json;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires live MinIO+Postgres; run with UDB_LIVE_OBJECT_TESTS=1 ... -- --ignored"]
async fn live_minio_storage_object_delete_roundtrip() {
    // Configure only Postgres + MinIO; other backends stay unregistered.
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

    let runtime = DataBrokerRuntime::from_env().await;
    let bucket = std::env::var("UDB_STORAGE_BUCKET").unwrap_or_else(|_| "udb-storage".to_string());
    let key = format!("ittest/{}.txt", Uuid::new_v4());

    // put
    let put_req = object_request_json("put", &bucket, &key, "text/plain");
    runtime
        .put_object_backend_target("minio", None, &put_req, b"hello-udb".to_vec())
        .await
        .expect("put_object");

    // get → bytes round-trip
    let get_req = object_request_json("get", &bucket, &key, "");
    let got = runtime
        .get_object_backend_target("minio", None, &get_req)
        .await
        .expect("get_object");
    assert_eq!(got, b"hello-udb", "object bytes must round-trip");

    // delete (the path DeleteFile/reaper drive)
    let del_req = object_request_json("delete", &bucket, &key, "");
    runtime
        .delete_object_backend_target("minio", None, "default", &del_req)
        .await
        .expect("delete_object");

    // get → gone
    let after = runtime
        .get_object_backend_target("minio", None, &get_req)
        .await;
    assert!(after.is_err(), "object must be gone after delete");
}

/// Verifies the presigned-URL path the storage service uses for
/// `RegisterUpload` (PUT) / `GetDownloadUrl` (GET): mint a PUT URL, upload bytes
/// over plain HTTP to it, confirm via the object path, then mint a GET URL and
/// download over HTTP.
#[tokio::test]
#[ignore = "requires live MinIO+Postgres; run with UDB_LIVE_OBJECT_TESTS=1 ... -- --ignored"]
async fn live_minio_presign_put_get_roundtrip() {
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

    let runtime = DataBrokerRuntime::from_env().await;
    let bucket = std::env::var("UDB_STORAGE_BUCKET").unwrap_or_else(|_| "udb-storage".to_string());
    let key = format!("presign/{}.txt", Uuid::new_v4());
    let http = reqwest::Client::new();

    // mint presigned PUT and upload bytes directly to the object store
    let (put_url, _) = runtime
        .presign_object_backend_target(None, "default", &bucket, &key, "PUT", "text/plain", 300)
        .await
        .expect("presign PUT");
    assert!(
        put_url.starts_with("http"),
        "presigned PUT must be an http url"
    );
    let resp = http
        .put(&put_url)
        .header("content-type", "text/plain")
        .body("via-presign")
        .send()
        .await
        .expect("http PUT");
    assert!(
        resp.status().is_success(),
        "presigned PUT failed: {}",
        resp.status()
    );

    // confirm the bytes landed via the object path
    let got = runtime
        .get_object_backend_target(
            "minio",
            None,
            &object_request_json("get", &bucket, &key, ""),
        )
        .await
        .expect("get after presigned put");
    assert_eq!(got, b"via-presign");

    // mint presigned GET and download over HTTP
    let (get_url, _) = runtime
        .presign_object_backend_target(None, "default", &bucket, &key, "GET", "", 300)
        .await
        .expect("presign GET");
    let body = http
        .get(&get_url)
        .send()
        .await
        .expect("http GET")
        .bytes()
        .await
        .expect("http GET body");
    assert_eq!(&body[..], b"via-presign");

    // cleanup
    let _ = runtime
        .delete_object_backend_target(
            "minio",
            None,
            "default",
            &object_request_json("delete", &bucket, &key, ""),
        )
        .await;
}
