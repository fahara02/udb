//! G10: live seam test for the notification HTTP sender against a real Postgres
//! and a real HTTP provider.
//!
//! Drives the production delivery pass (`run_notification_delivery_worker_pass`
//! → `run_notification_delivery_once`: durable PENDING log scan → SSRF guard →
//! credential decrypt → POST → attempt row + parent transition + events) and
//! asserts, at the provider and in the notification tables:
//! - a 2xx provider receives the rendered `{to,subject,body}` POST with the
//!   bearer credential and the stable `<log_id>:<channel>` idempotency key; the
//!   log becomes SENT and one SENT attempt row records the provider message-id;
//! - a 503 provider leaves the log PENDING with a FAILED attempt (retry
//!   scheduled behind the backoff gate), and once the bounded retry budget is
//!   spent the log moves to the terminal FAILED state.
//!
//! The provider is a local loopback `tokio::net::TcpListener`. Production
//! delivery refuses cleartext and loopback endpoints (SSRF guard), so the test
//! flips the test-build-only notification `ALLOW_LOOPBACK_HTTP_DELIVERY_FOR_TEST`
//! switch for the duration of the passes.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;
use uuid::Uuid;

use super::support::{
    live_native_service_db_lock, live_pg_pool, live_runtime, migrate_native_service_db,
    require_live_dsn_any,
};
use crate::proto::udb::core::notification::entity::v1::NotificationChannel;
use crate::runtime::service::notification_service::{
    ALLOW_LOOPBACK_HTTP_DELIVERY_FOR_TEST, NotificationDeliveryIntent,
    NotificationDeliveryProvider, ProviderAuth, max_delivery_attempts,
    run_notification_delivery_once, run_notification_delivery_worker_pass,
};

const PROVIDER_MESSAGE_ID: &str = "prov-msg-42";

/// Holds the loopback switch ON and turns it back OFF on drop, so a panicking
/// assertion can never leak cleartext-loopback delivery into a later test.
struct LoopbackSwitch;

impl LoopbackSwitch {
    fn enable() -> Self {
        ALLOW_LOOPBACK_HTTP_DELIVERY_FOR_TEST.store(true, Ordering::SeqCst);
        Self
    }
}

impl Drop for LoopbackSwitch {
    fn drop(&mut self) {
        ALLOW_LOOPBACK_HTTP_DELIVERY_FOR_TEST.store(false, Ordering::SeqCst);
    }
}

/// One request the loopback provider accepted.
#[derive(Debug, Clone)]
pub(in crate::runtime::service) struct Received {
    pub(in crate::runtime::service) method: String,
    pub(in crate::runtime::service) path: String,
    pub(in crate::runtime::service) headers: HashMap<String, String>,
    pub(in crate::runtime::service) body: Vec<u8>,
}

/// Minimal HTTP/1.1 provider: reads one request per connection (headers +
/// `content-length` body), records it, answers with `status_line` (plus an
/// `x-message-id` header) and closes.
pub(in crate::runtime::service) async fn spawn_provider(
    status_line: &'static str,
) -> (std::net::SocketAddr, Arc<Mutex<Vec<Received>>>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback notification provider");
    let addr = listener.local_addr().expect("provider address");
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
                let mut request_line = lines.next().unwrap_or_default().split_whitespace();
                let method = request_line.next().unwrap_or_default().to_string();
                let path = request_line.next().unwrap_or_default().to_string();
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
                    method,
                    path,
                    headers,
                    body,
                });
                let response = format!(
                    "HTTP/1.1 {status_line}\r\nx-message-id: {PROVIDER_MESSAGE_ID}\r\n\
                     content-length: 0\r\nconnection: close\r\n\r\n"
                );
                let _ = socket.write_all(response.as_bytes()).await;
                let _ = socket.shutdown().await;
            });
        }
    });
    (addr, received)
}

fn email_provider(
    addr: std::net::SocketAddr,
    path: &str,
    wrapped: &str,
) -> NotificationDeliveryProvider {
    NotificationDeliveryProvider {
        channel: NotificationChannel::Email as i32,
        provider: "live-http".to_string(),
        endpoint_url: format!("http://{addr}{path}"),
        wrapped_credential: wrapped.to_string(),
        body_template: None,
        auth: ProviderAuth::Bearer,
        idempotency_header: "Idempotency-Key".to_string(),
        message_id_header: Some("x-message-id".to_string()),
        message_id_json_path: None,
    }
}

/// Queue one PENDING EMAIL notification log (the durable intent the worker
/// drains) inside `project`.
async fn queue_email(pool: &sqlx::PgPool, tenant: &str, project: &str, to: &str) -> String {
    let log_id = Uuid::new_v4().to_string();
    sqlx::query(
        "INSERT INTO udb_notification.notification_logs \
            (log_id, event_type, channel, recipient_address, tenant_id, project_id, \
             status, rendered_subject, rendered_body, created_at) \
         VALUES ($1::UUID, 'live.http.delivery', 'EMAIL', $2, $3, $4, \
             'PENDING', 'Live subject', 'Live body', NOW())",
    )
    .bind(&log_id)
    .bind(to)
    .bind(tenant)
    .bind(project)
    .execute(pool)
    .await
    .expect("insert queued notification");
    log_id
}

async fn log_status(pool: &sqlx::PgPool, log_id: &str) -> String {
    sqlx::query_scalar::<_, String>(
        "SELECT status FROM udb_notification.notification_logs WHERE log_id = $1::UUID",
    )
    .bind(log_id)
    .fetch_one(pool)
    .await
    .expect("load notification status")
}

/// `(status, attempt_count, last_error, provider_message_id)` for every attempt
/// row of `log_id`.
async fn attempts(pool: &sqlx::PgPool, log_id: &str) -> Vec<(String, i32, String, String)> {
    sqlx::query_as(
        "SELECT status::TEXT, attempt_count, COALESCE(last_error, ''), \
                COALESCE(provider_message_id, '') \
         FROM udb_notification.notification_delivery_attempts \
         WHERE notification_id = $1::UUID",
    )
    .bind(log_id)
    .fetch_all(pool)
    .await
    .expect("load delivery attempts")
}

#[tokio::test]
#[ignore = "requires live Postgres; runs in the CI live lane"]
async fn notification_http_sender_delivers_and_retries_live() {
    if require_live_dsn_any(&[
        "UDB_LIVE_NATIVE_PG_DSN",
        "UDB_LIVE_AUTH_PG_DSN",
        "UDB_INTEGRATION_PG_DSN",
    ])
    .is_none()
    {
        eprintln!("live Postgres DSN unset — skipping notification HTTP delivery seam");
        return;
    }
    let _guard = live_native_service_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_service_db(&pool).await;
    let runtime = live_runtime().await;
    let outbox = runtime.config().cdc.outbox_relation();
    // Never let an ambient HTTP(S)_PROXY intercept the loopback provider.
    let http = reqwest::Client::builder()
        .no_proxy()
        .build()
        .expect("build delivery http client");

    let token = format!("prov-token-{}", Uuid::new_v4().simple());
    // Sealed exactly as an operator stores it; with no at-rest key configured
    // (or fail-closed refusing to seal) the plaintext round-trips unchanged.
    let wrapped = runtime
        .encrypt_secret_at_rest(&token)
        .unwrap_or_else(|_| token.clone());
    let tenant = Uuid::new_v4().to_string();

    let loopback = LoopbackSwitch::enable();

    // --- Case 1: 2xx provider → SENT ------------------------------------------
    let (ok_addr, ok_received) = spawn_provider("200 OK").await;
    let ok_providers = vec![email_provider(ok_addr, "/send", &wrapped)];
    let ok_project = "notification-http-live-ok";
    let ok_log = queue_email(&pool, &tenant, ok_project, "ops@example.test").await;
    let delivered = run_notification_delivery_worker_pass(
        &http,
        runtime.clone(),
        &pool,
        ok_project,
        Some(&outbox),
        50,
        None,
        &ok_providers,
        3600,
    )
    .await
    .expect("2xx delivery pass");

    // --- Case 2: 503 provider → retry scheduled, then terminal FAILED ----------
    let (bad_addr, bad_received) = spawn_provider("503 Service Unavailable").await;
    let bad_providers = vec![email_provider(bad_addr, "/send", &wrapped)];
    let bad_project = "notification-http-live-503";
    let bad_log = queue_email(&pool, &tenant, bad_project, "retry@example.test").await;
    let first_bad = run_notification_delivery_worker_pass(
        &http,
        runtime.clone(),
        &pool,
        bad_project,
        Some(&outbox),
        50,
        None,
        &bad_providers,
        3600,
    )
    .await
    .expect("first 503 delivery pass");
    let bad_status_after_first = log_status(&pool, &bad_log).await;
    let bad_attempts_after_first = attempts(&pool, &bad_log).await;
    // Immediately re-running the worker pass must NOT re-POST: the failed
    // attempt sits behind the exponential-backoff gate (base 5s, jittered).
    let gated = run_notification_delivery_worker_pass(
        &http,
        runtime.clone(),
        &pool,
        bad_project,
        Some(&outbox),
        50,
        None,
        &bad_providers,
        3600,
    )
    .await
    .expect("backoff-gated delivery pass");
    let posts_after_gate = bad_received.lock().await.len();
    // Spend the rest of the bounded retry budget by driving the sender directly
    // (bypassing the backoff scan) with the same durable intent.
    let max_attempts = max_delivery_attempts();
    let intent = NotificationDeliveryIntent {
        log_id: bad_log.clone(),
        template_id: String::new(),
        tenant_id: tenant.clone(),
        project_id: bad_project.to_string(),
        channel: NotificationChannel::Email as i32,
        recipient_id: String::new(),
        event_type: "live.http.delivery".to_string(),
        correlation_id: String::new(),
        recipient_address: "retry@example.test".to_string(),
        rendered_subject: "Live subject".to_string(),
        rendered_body: "Live body".to_string(),
    };
    for _ in 1..max_attempts.clamp(1, 20) {
        let sent = run_notification_delivery_once(
            &http,
            runtime.as_ref(),
            &pool,
            Some(&outbox),
            &bad_providers,
            std::slice::from_ref(&intent),
            None,
        )
        .await
        .expect("503 retry delivery");
        assert_eq!(sent, 0, "a 503 provider never counts as delivered");
    }

    // A reset code reaches its provider, then both rendered fields are scrubbed
    // from the durable log once no retry needs them.
    let (secret_addr, secret_received) = spawn_provider("200 OK").await;
    let secret_project = "notification-http-live-authn";
    let secret_log = queue_email(&pool, &tenant, secret_project, "reset@example.test").await;
    sqlx::query("UPDATE udb_notification.notification_logs SET event_type = 'authn.password_reset', rendered_subject = 'Reset 123456', rendered_body = 'Code 123456' WHERE log_id = $1::UUID")
        .bind(&secret_log).execute(&pool).await.expect("queue a secret-bearing body");
    assert_eq!(
        run_notification_delivery_worker_pass(
            &http,
            runtime.clone(),
            &pool,
            secret_project,
            Some(&outbox),
            50,
            None,
            &[email_provider(secret_addr, "/send", &wrapped)],
            3600,
        )
        .await
        .expect("auth-code delivery pass"),
        1
    );
    let secret_posts = secret_received.lock().await.clone();
    assert_eq!(secret_posts.len(), 1);
    let secret_body: serde_json::Value =
        serde_json::from_slice(&secret_posts[0].body).expect("provider JSON");
    assert_eq!(secret_body["subject"], "Reset 123456");
    assert_eq!(secret_body["body"], "Code 123456");
    let scrubbed: (String, String) = sqlx::query_as("SELECT rendered_subject, rendered_body FROM udb_notification.notification_logs WHERE log_id = $1::UUID")
        .bind(&secret_log).fetch_one(&pool).await.expect("read back delivered auth code");
    assert!(
        !scrubbed.0.contains("123456") && !scrubbed.1.contains("123456"),
        "terminal delivery removes the stored code"
    );

    drop(loopback);
    assert!(!ALLOW_LOOPBACK_HTTP_DELIVERY_FOR_TEST.load(Ordering::SeqCst));

    // Case 1 assertions.
    assert_eq!(delivered, 1, "the queued EMAIL must be delivered once");
    let ok_posts = ok_received.lock().await.clone();
    assert_eq!(ok_posts.len(), 1, "exactly one provider POST: {ok_posts:?}");
    let post = &ok_posts[0];
    assert_eq!(post.method, "POST");
    assert_eq!(post.path, "/send");
    assert_eq!(
        post.headers.get("authorization").map(String::as_str),
        Some(format!("Bearer {token}").as_str()),
        "the decrypted provider credential is sent as the bearer token"
    );
    assert_eq!(
        post.headers.get("idempotency-key").map(String::as_str),
        Some(format!("{ok_log}:EMAIL").as_str()),
        "the stable <log_id>:<channel> idempotency key is sent"
    );
    assert!(
        post.headers
            .get("content-type")
            .is_some_and(|v| v.starts_with("application/json")),
        "{post:?}"
    );
    let body: serde_json::Value =
        serde_json::from_slice(&post.body).expect("provider body is JSON");
    assert_eq!(
        body,
        serde_json::json!({
            "to": "ops@example.test",
            "subject": "Live subject",
            "body": "Live body",
        })
    );
    assert_eq!(log_status(&pool, &ok_log).await, "SENT");
    let ok_attempts = attempts(&pool, &ok_log).await;
    assert_eq!(ok_attempts.len(), 1, "one attempt row: {ok_attempts:?}");
    assert_eq!(ok_attempts[0].0, "SENT");
    assert_eq!(ok_attempts[0].1, 1);
    assert_eq!(ok_attempts[0].2, "");
    assert_eq!(
        ok_attempts[0].3, PROVIDER_MESSAGE_ID,
        "the provider's real message-id is read from the configured header"
    );

    // Case 2 assertions.
    assert_eq!(first_bad, 0, "a 503 provider never counts as delivered");
    assert_eq!(gated, 0);
    assert_eq!(
        bad_attempts_after_first.len(),
        1,
        "one attempt row after the first 503: {bad_attempts_after_first:?}"
    );
    assert_eq!(bad_attempts_after_first[0].0, "FAILED");
    assert_eq!(bad_attempts_after_first[0].1, 1);
    assert!(
        bad_attempts_after_first[0].2.contains("503"),
        "the attempt records the provider status: {bad_attempts_after_first:?}"
    );
    if max_attempts > 1 {
        assert_eq!(
            bad_status_after_first, "PENDING",
            "a retryable failure under the retry budget stays queued for retry"
        );
        assert_eq!(
            posts_after_gate, 1,
            "the backoff gate must hold the failed intent back from an immediate re-POST"
        );
    }
    let bad_posts = bad_received.lock().await.len();
    let expected_posts = usize::try_from(max_attempts.clamp(1, 20)).unwrap_or(1);
    assert_eq!(
        bad_posts, expected_posts,
        "one POST per attempt up to the retry budget"
    );
    let final_attempts = attempts(&pool, &bad_log).await;
    assert_eq!(final_attempts.len(), 1, "{final_attempts:?}");
    assert_eq!(final_attempts[0].0, "FAILED");
    assert_eq!(
        i64::from(final_attempts[0].1),
        max_attempts.clamp(1, 20),
        "attempt_count counts every POST"
    );
    if max_attempts <= 20 {
        assert_eq!(
            log_status(&pool, &bad_log).await,
            "FAILED",
            "an exhausted retry budget moves the log to terminal FAILED"
        );
    }
}
