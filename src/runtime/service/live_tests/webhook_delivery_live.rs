//! D16: live seam test for the webhook DELIVERY worker against a real Postgres
//! and a real HTTP receiver.
//!
//! Drives the production tick `run_webhook_delivery_worker_once` (journal →
//! endpoint join → secret decrypt → sign → POST → delivery journal) and
//! asserts, at the receiver and in the delivery table:
//! - a journal event published BEFORE the endpoint existed is never delivered
//!   (the delivery window starts at endpoint creation);
//! - a newer event arrives with an `x-udb-signature` HMAC the receiver can
//!   verify with the endpoint's secret over `"<x-udb-timestamp>.<body>"`;
//! - an endpoint whose stored secret cannot be decrypted is dead-lettered
//!   without any POST (never signed with ciphertext).
//!
//! The receiver is a local loopback `tokio::net::TcpListener`. Production
//! delivery refuses cleartext and loopback targets (SSRF guard), so the test
//! flips the test-build-only `ALLOW_LOOPBACK_HTTP_DELIVERY_FOR_TEST` switch for
//! the duration of the tick.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;
use uuid::Uuid;

use super::support::{
    live_native_service_db_lock, live_pg_pool, live_runtime, migrate_native_service_db,
    require_live_dsn_any,
};
use crate::runtime::native_catalog::native_model;
use crate::runtime::service::webhook_service::{
    ALLOW_LOOPBACK_HTTP_DELIVERY_FOR_TEST, run_webhook_delivery_worker_once,
};
use crate::runtime::system::SystemCatalogConfig;

const ENDPOINT_MSG: &str = "udb.core.webhook.entity.v1.WebhookEndpoint";
const DELIVERY_MSG: &str = "udb.core.webhook.entity.v1.WebhookDelivery";
const TOPIC: &str = "acme.orders.created.v1";

/// One request the loopback receiver accepted.
#[derive(Debug, Clone)]
struct Received {
    path: String,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

/// Minimal HTTP/1.1 receiver: reads one request per connection (headers +
/// `content-length` body), records it, answers `200 OK` and closes.
async fn spawn_receiver() -> (std::net::SocketAddr, Arc<Mutex<Vec<Received>>>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback webhook receiver");
    let addr = listener.local_addr().expect("receiver address");
    let received = Arc::new(Mutex::new(Vec::new()));
    let sink = received.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let sink = sink.clone();
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let mut chunk = [0u8; 4096];
                let header_end = loop {
                    let Ok(n) = socket.read(&mut chunk).await else {
                        return;
                    };
                    if n == 0 {
                        return;
                    }
                    buf.extend_from_slice(&chunk[..n]);
                    if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                        break pos + 4;
                    }
                };
                let head = String::from_utf8_lossy(&buf[..header_end]).to_string();
                let mut lines = head.split("\r\n");
                let path = lines
                    .next()
                    .and_then(|line| line.split_whitespace().nth(1))
                    .unwrap_or_default()
                    .to_string();
                let headers: HashMap<String, String> = lines
                    .filter_map(|line| line.split_once(':'))
                    .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_string()))
                    .collect();
                let content_length: usize = headers
                    .get("content-length")
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(0);
                let mut body = buf[header_end..].to_vec();
                while body.len() < content_length {
                    let Ok(n) = socket.read(&mut chunk).await else {
                        break;
                    };
                    if n == 0 {
                        break;
                    }
                    body.extend_from_slice(&chunk[..n]);
                }
                body.truncate(content_length);
                sink.lock().await.push(Received {
                    path,
                    headers,
                    body,
                });
                let _ = socket
                    .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")
                    .await;
                let _ = socket.shutdown().await;
            });
        }
    });
    (addr, received)
}

fn hex_lower(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

async fn insert_endpoint(
    pool: &sqlx::PgPool,
    tenant_id: &str,
    url: &str,
    stored_secret: &str,
) -> String {
    let m = native_model(
        ENDPOINT_MSG,
        &[
            "endpoint_id",
            "tenant_id",
            "url",
            "topic_pattern",
            "signing_secret",
            "active",
            "max_attempts",
            "metadata_json",
        ],
    );
    let endpoint_id = Uuid::new_v4().to_string();
    sqlx::query(&format!(
        "INSERT INTO {rel} \
         ({endpoint_id}, {tenant_id}, {url}, {topic_pattern}, {signing_secret}, {active}, {max_attempts}, {metadata_json}) \
         VALUES ($1::UUID, $2::UUID, $3, $4, $5, true, 1, '{{}}'::JSONB)",
        rel = m.relation,
        endpoint_id = m.q("endpoint_id"),
        tenant_id = m.q("tenant_id"),
        url = m.q("url"),
        topic_pattern = m.q("topic_pattern"),
        signing_secret = m.q("signing_secret"),
        active = m.q("active"),
        max_attempts = m.q("max_attempts"),
        metadata_json = m.q("metadata_json"),
    ))
    .bind(&endpoint_id)
    .bind(tenant_id)
    .bind(url)
    .bind("acme.orders.*")
    .bind(stored_secret)
    .execute(pool)
    .await
    .expect("insert webhook endpoint");
    endpoint_id
}

/// Journal one published change event for `tenant_id`, `offset_secs` relative
/// to NOW() (negative = in the past).
async fn journal_event(
    pool: &sqlx::PgPool,
    tenant_id: &str,
    marker: &str,
    offset_secs: f64,
) -> String {
    let event_id = Uuid::new_v4();
    let journal = SystemCatalogConfig::current().cdc_journal_relation();
    sqlx::query(&format!(
        "INSERT INTO {journal} \
         (event_id, topic, partition_key, payload, published_at, delivery_state) \
         VALUES ($1, $2, 'webhook-live', $3::JSONB, \
                 NOW() + make_interval(secs => $4), 'published')"
    ))
    .bind(event_id)
    .bind(TOPIC)
    .bind(
        serde_json::json!({"tenant_id": tenant_id, "marker": marker, "payload": {"order": marker}})
            .to_string(),
    )
    .bind(offset_secs)
    .execute(pool)
    .await
    .expect("insert webhook journal event");
    event_id.to_string()
}

async fn delivery_rows(pool: &sqlx::PgPool, endpoint_id: &str) -> Vec<(String, String, String)> {
    let m = native_model(
        DELIVERY_MSG,
        &["endpoint_id", "event_id", "status", "last_error"],
    );
    sqlx::query_as(&format!(
        "SELECT {event_id}, {status}, COALESCE({last_error}, '') FROM {rel} \
         WHERE {endpoint}::TEXT = $1 ORDER BY {event_id}",
        rel = m.relation,
        event_id = m.q("event_id"),
        status = m.q("status"),
        last_error = m.q("last_error"),
        endpoint = m.q("endpoint_id"),
    ))
    .bind(endpoint_id)
    .fetch_all(pool)
    .await
    .expect("read webhook delivery rows")
}

#[tokio::test]
#[ignore = "requires live Postgres (UDB_INTEGRATION_PG_DSN); runs in the CI --ignored live lane"]
async fn webhook_worker_windows_signs_and_dead_letters_unsignable_live() {
    if require_live_dsn_any(&[
        "UDB_LIVE_NATIVE_PG_DSN",
        "UDB_LIVE_AUTH_PG_DSN",
        "UDB_INTEGRATION_PG_DSN",
    ])
    .is_none()
    {
        eprintln!("live Postgres DSN unset — skipping webhook delivery worker seam");
        return;
    }
    let _guard = live_native_service_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_service_db(&pool).await;
    let runtime = live_runtime().await;
    let (addr, received) = spawn_receiver().await;

    let tenant = Uuid::new_v4().to_string();
    // History from BEFORE the endpoint existed: must never be delivered.
    let old_event = journal_event(&pool, &tenant, "before-endpoint", -3600.0).await;

    let secret = format!("whsec-{}", Uuid::new_v4().simple());
    let sealed = runtime
        .encrypt_secret_at_rest(&secret)
        .expect("seal webhook signing secret");
    let good = insert_endpoint(&pool, &tenant, &format!("http://{addr}/hook-good"), &sealed).await;
    // A sealed-looking secret no configured key can open.
    let unsignable = insert_endpoint(
        &pool,
        &tenant,
        &format!("http://{addr}/hook-unsignable"),
        "udb-aead:v1:not-a-real-envelope",
    )
    .await;

    // Published after both endpoints were created.
    let new_event = journal_event(&pool, &tenant, "after-endpoint", 1.0).await;

    ALLOW_LOOPBACK_HTTP_DELIVERY_FOR_TEST.store(true, Ordering::SeqCst);
    let journal = SystemCatalogConfig::current().cdc_journal_relation();
    let delivered = tokio::time::timeout(
        Duration::from_secs(60),
        run_webhook_delivery_worker_once(
            &reqwest::Client::new(),
            &pool,
            None,
            &journal,
            200,
            None,
            runtime.as_ref(),
        ),
    )
    .await
    .expect("webhook delivery tick must finish");
    ALLOW_LOOPBACK_HTTP_DELIVERY_FOR_TEST.store(false, Ordering::SeqCst);
    let delivered = delivered.expect("webhook delivery tick");
    assert_eq!(delivered, 1, "exactly the post-creation event is delivered");

    let requests = received.lock().await.clone();
    assert_eq!(
        requests.len(),
        1,
        "one POST (good endpoint, new event): {requests:?}"
    );
    let request = &requests[0];
    assert_eq!(
        request.path, "/hook-good",
        "the unsignable endpoint is never POSTed"
    );
    let body: serde_json::Value =
        serde_json::from_slice(&request.body).expect("delivered body is JSON");
    assert_eq!(body["marker"], "after-endpoint", "{body}");

    // The receiver verifies the signature with the endpoint secret.
    let timestamp = request
        .headers
        .get("x-udb-timestamp")
        .expect("timestamp header")
        .clone();
    let signature = request
        .headers
        .get("x-udb-signature")
        .expect("signature header")
        .clone();
    let mut signed = timestamp.clone().into_bytes();
    signed.push(b'.');
    signed.extend_from_slice(&request.body);
    let expected = format!(
        "sha256={}",
        hex_lower(&crate::runtime::security::hmac_sha256(
            secret.as_bytes(),
            &signed
        ))
    );
    assert_eq!(
        signature, expected,
        "signature must verify with the plaintext secret"
    );

    // Ledger: the good endpoint has exactly one DELIVERED row (the new event);
    // the pre-creation event has none.
    let good_rows = delivery_rows(&pool, &good).await;
    assert_eq!(
        good_rows,
        vec![(new_event.clone(), "DELIVERED".to_string(), String::new())],
        "old event {old_event} must not be journaled for the endpoint"
    );
    // The unsignable endpoint's delivery is dead-lettered, unsent.
    let dead_rows = delivery_rows(&pool, &unsignable).await;
    assert_eq!(dead_rows.len(), 1, "{dead_rows:?}");
    assert_eq!(dead_rows[0].0, new_event);
    assert_eq!(dead_rows[0].1, "DEAD");
    assert!(
        dead_rows[0].2.contains("signing secret"),
        "dead-letter reason names the secret: {:?}",
        dead_rows[0].2
    );

    // A second tick re-delivers nothing (terminal rows exist for both).
    ALLOW_LOOPBACK_HTTP_DELIVERY_FOR_TEST.store(true, Ordering::SeqCst);
    let again = run_webhook_delivery_worker_once(
        &reqwest::Client::new(),
        &pool,
        None,
        &journal,
        200,
        None,
        runtime.as_ref(),
    )
    .await;
    ALLOW_LOOPBACK_HTTP_DELIVERY_FOR_TEST.store(false, Ordering::SeqCst);
    assert_eq!(again.expect("second webhook tick"), 0);
    assert_eq!(
        received.lock().await.len(),
        1,
        "the second tick must not POST again"
    );
}
