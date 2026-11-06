//! The vault leases OAuth tokens from a stored credential against a real HTTP token endpoint:
//! a usable token is reused, an expiring one is refreshed once however many callers ask, and a
//! rotated refresh token is what the next refresh sends.

use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tonic::Request;
use uuid::Uuid;

use super::support::*;
use crate::proto::udb::core::vault::services::v1 as vault_pb;
use crate::proto::udb::core::vault::services::v1::vault_service_server::VaultService;

/// A token endpoint that records each request body and rotates its refresh token.
async fn token_endpoint(lifetime: i64) -> (String, Arc<Mutex<Vec<String>>>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let url = format!("http://{}/token", listener.local_addr().expect("addr"));
    let bodies = Arc::new(Mutex::new(Vec::new()));
    let seen = bodies.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let mut buffer = vec![0u8; 8192];
            let mut read = 0;
            loop {
                let n = socket.read(&mut buffer[read..]).await.unwrap_or(0);
                read += n;
                let text = String::from_utf8_lossy(&buffer[..read]).to_string();
                let complete = text.split_once("\r\n\r\n").is_some_and(|(head, body)| {
                    head.lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .map(|value| value.trim().parse::<usize>().unwrap_or(0))
                        })
                        .is_some_and(|length| body.len() >= length)
                });
                if n == 0 || complete {
                    break;
                }
            }
            let text = String::from_utf8_lossy(&buffer[..read]).to_string();
            let body = text
                .split_once("\r\n\r\n")
                .map_or("", |(_, b)| b)
                .to_string();
            let count = {
                let mut bodies = seen.lock().unwrap();
                bodies.push(body);
                bodies.len()
            };
            // A slow provider, so waiting callers really overlap the refresh.
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            let answer = format!(
                r#"{{"access_token":"access-{count}","expires_in":{lifetime},"refresh_token":"refresh-{count}"}}"#
            );
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{answer}",
                answer.len()
            );
            let _ = socket.write_all(response.as_bytes()).await;
        }
    });
    (url, bodies)
}

fn scoped<T>(message: T, tenant: &str) -> Request<T> {
    let mut request = Request::new(message);
    let metadata = request.metadata_mut();
    metadata.insert("x-tenant-id", tenant.parse().expect("tenant"));
    metadata.insert("x-udb-project-id", "default".parse().expect("project"));
    metadata.insert(
        "x-request-id",
        Uuid::new_v4().to_string().parse().expect("request id"),
    );
    request
}

#[tokio::test]
#[ignore = "requires live Postgres; runs in the CI native live lane (UDB_LIVE_AUTH_TESTS=1 ... -- --ignored)"]
async fn the_vault_leases_and_refreshes_an_oauth_token_once_live() {
    let _guard = live_native_service_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_service_db(&pool).await;
    // SAFETY: this test holds the live lock, and nothing else reads this variable.
    unsafe { std::env::set_var("UDB_VAULT_OAUTH_ALLOW_PRIVATE_TOKEN_URL", "true") };
    let service = Arc::new(vault_service().await);
    let tenant = Uuid::new_v4().to_string();
    let path = format!("oauth/{}", Uuid::new_v4().simple());
    let (url, bodies) = token_endpoint(3600).await;
    service
        .put_secret(scoped(
            vault_pb::PutSecretRequest {
                tenant_id: tenant.clone(),
                secret_path: path.clone(),
                secret_value: serde_json::json!({
                    "token_url": url,
                    "client_id": "client",
                    "client_secret": "s3cret",
                    "refresh_token": "refresh-0",
                })
                .to_string(),
                expected_version: 0,
                ..Default::default()
            },
            &tenant,
        ))
        .await
        .expect("store the credential");
    let lease = |min_valid: i32| {
        let service = service.clone();
        let tenant = tenant.clone();
        let path = path.clone();
        async move {
            service
                .lease_o_auth_token(scoped(
                    vault_pb::LeaseOAuthTokenRequest {
                        tenant_id: tenant.clone(),
                        secret_path: path,
                        min_valid_seconds: min_valid,
                    },
                    &tenant,
                ))
                .await
                .expect("a lease")
                .into_inner()
        }
    };

    // Many callers, one refresh.
    let leases = futures::future::join_all((0..5).map(|_| lease(60))).await;
    assert_eq!(
        bodies.lock().unwrap().len(),
        1,
        "one refresh for all callers"
    );
    assert!(leases.iter().all(|l| l.access_token == "access-1"));
    assert_eq!(leases.iter().filter(|l| l.refreshed).count(), 1);
    assert!(bodies.lock().unwrap()[0].contains("refresh_token=refresh-0"));

    // A usable token is reused.
    let again = lease(60).await;
    assert_eq!(again.access_token, "access-1");
    assert!(!again.refreshed);

    // A margin it cannot meet refreshes, with the rotated token.
    let renewed = lease(7200).await;
    assert_eq!(renewed.access_token, "access-2");
    assert!(renewed.refreshed);
    assert!(bodies.lock().unwrap()[1].contains("refresh_token=refresh-1"));

    // The secret holds no token in clear.
    let stored: Vec<u8> = sqlx::query_scalar(
        "SELECT ciphertext::BYTEA FROM udb_vault.vault_secrets \
         WHERE tenant_id = $1 AND secret_path = $2 ORDER BY version DESC LIMIT 1",
    )
    .bind(&tenant)
    .bind(&path)
    .fetch_one(&pool)
    .await
    .expect("the stored version");
    assert!(!String::from_utf8_lossy(&stored).contains("access-2"));
}

#[tokio::test]
#[ignore = "requires live Postgres; runs in the CI native live lane (UDB_LIVE_AUTH_TESTS=1 ... -- --ignored)"]
async fn a_private_token_url_is_refused_outside_development_live() {
    let _guard = live_native_service_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_service_db(&pool).await;
    // SAFETY: this test holds the live lock, and nothing else reads this variable.
    unsafe { std::env::remove_var("UDB_VAULT_OAUTH_ALLOW_PRIVATE_TOKEN_URL") };
    let service = vault_service().await;
    let tenant = Uuid::new_v4().to_string();
    let path = format!("oauth/{}", Uuid::new_v4().simple());
    service
        .put_secret(scoped(
            vault_pb::PutSecretRequest {
                tenant_id: tenant.clone(),
                secret_path: path.clone(),
                secret_value: serde_json::json!({
                    "token_url": "http://169.254.169.254/latest/meta-data",
                    "client_id": "client",
                })
                .to_string(),
                expected_version: 0,
                ..Default::default()
            },
            &tenant,
        ))
        .await
        .expect("store the credential");
    let refused = service
        .lease_o_auth_token(scoped(
            vault_pb::LeaseOAuthTokenRequest {
                tenant_id: tenant.clone(),
                secret_path: path,
                min_valid_seconds: 0,
            },
            &tenant,
        ))
        .await
        .expect_err("no request leaves the broker");
    assert_eq!(refused.code(), tonic::Code::InvalidArgument);
}
