//! OAuth tokens leased from a stored credential.
//!
//! The credential is an ordinary secret, so it is sealed, versioned and audited like any other,
//! and the tokens it yields are stored as its next version. A provider that rotates refresh
//! tokens invalidates the old one on first use, so only one caller may refresh at a time: an
//! advisory lock covers the replicas, and the waiter re-reads the version the winner stored.

use serde::{Deserialize, Serialize};
use tonic::{Request, Response, Status};

use crate::proto::udb::core::vault::services::v1 as vault_pb;

use super::VaultServiceImpl;
use super::errors::{vault_field_violation, vault_internal_status, vault_required_secret_path};
use super::handlers::{get_secret, put_secret};

const DEFAULT_MIN_VALID_SECONDS: i64 = 60;
const LOCK_WAIT: std::time::Duration = std::time::Duration::from_secs(15);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct OAuthCredential {
    pub(crate) token_url: String,
    pub(crate) client_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub(crate) client_secret: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub(crate) refresh_token: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub(crate) scope: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub(crate) access_token: String,
    #[serde(default)]
    pub(crate) expires_at_unix: i64,
}

impl OAuthCredential {
    pub(crate) fn parse(raw: &str) -> Result<Self, Status> {
        let credential: Self = serde_json::from_str(raw).map_err(|_| {
            vault_field_violation(
                "secret_path",
                "must hold a JSON OAuth credential with token_url and client_id",
                "the secret is not an OAuth credential",
            )
        })?;
        if credential.token_url.trim().is_empty() || credential.client_id.trim().is_empty() {
            return Err(vault_field_violation(
                "secret_path",
                "must hold a JSON OAuth credential with token_url and client_id",
                "the OAuth credential has no token_url or client_id",
            ));
        }
        Ok(credential)
    }

    /// A token with no stated expiry cannot be known valid, so it is fetched again.
    pub(crate) fn usable(&self, now_unix: i64, min_valid_seconds: i64) -> bool {
        !self.access_token.is_empty()
            && self.expires_at_unix > 0
            && self.expires_at_unix - now_unix >= min_valid_seconds
    }

    pub(crate) fn grant_form(&self) -> Vec<(&'static str, String)> {
        let mut form = vec![("client_id", self.client_id.clone())];
        if !self.client_secret.is_empty() {
            form.push(("client_secret", self.client_secret.clone()));
        }
        if self.refresh_token.is_empty() {
            form.push(("grant_type", "client_credentials".to_string()));
        } else {
            form.push(("grant_type", "refresh_token".to_string()));
            form.push(("refresh_token", self.refresh_token.clone()));
        }
        if !self.scope.is_empty() {
            form.push(("scope", self.scope.clone()));
        }
        form
    }

    /// The provider's answer applied: a refresh token it did not rotate is kept.
    pub(crate) fn apply_token_response(
        &mut self,
        body: &serde_json::Value,
        now_unix: i64,
    ) -> Result<(), Status> {
        let access_token = body
            .get("access_token")
            .and_then(serde_json::Value::as_str)
            .filter(|token| !token.is_empty())
            .ok_or_else(|| oauth_unavailable("the OAuth provider returned no access token"))?;
        self.access_token = access_token.to_string();
        // RFC 6749 sends a number; some providers send it as text.
        let expires_in = body.get("expires_in").and_then(|value| {
            value
                .as_i64()
                .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
        });
        self.expires_at_unix = expires_in.map_or(0, |seconds| now_unix + seconds);
        if let Some(rotated) = body
            .get("refresh_token")
            .and_then(serde_json::Value::as_str)
            .filter(|token| !token.is_empty())
        {
            self.refresh_token = rotated.to_string();
        }
        Ok(())
    }
}

/// The provider or the database is out of reach for now; the same call can succeed later.
fn oauth_unavailable(message: impl Into<String>) -> Status {
    crate::runtime::executor_utils::retryable_status("vault", "lease_oauth_token", 1_000, message)
}

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs() as i64)
}

/// A token endpoint must be public https, as a webhook target must. Development against a
/// local provider opts out, and production cannot.
fn validate_token_url(url: &str) -> Result<(), Status> {
    let local = std::env::var("UDB_VAULT_OAUTH_ALLOW_PRIVATE_TOKEN_URL").is_ok_and(|v| v == "true")
        && !crate::runtime::security::udb_env_is_production();
    if local {
        return Ok(());
    }
    crate::runtime::service::webhook_service::security::validate_webhook_target_url(url).map_err(
        |_| {
            vault_field_violation(
                "secret_path",
                "token_url must be a public https endpoint",
                "the OAuth credential's token_url is not a public https endpoint",
            )
        },
    )
}

#[cfg(feature = "http-client")]
async fn fetch_token(credential: &OAuthCredential) -> Result<serde_json::Value, Status> {
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .map_err(|_| {
            vault_internal_status(
                "lease_oauth_token_client",
                "the OAuth HTTP client could not start",
            )
        })?;
    let response = client
        .post(&credential.token_url)
        .header(reqwest::header::ACCEPT, "application/json")
        .form(&credential.grant_form())
        .send()
        .await
        .map_err(|_| oauth_unavailable("the OAuth provider could not be reached"))?;
    let status = response.status();
    let body: serde_json::Value = response.json().await.unwrap_or(serde_json::Value::Null);
    if status.is_client_error() {
        // The error code is the provider's protocol field, not a credential.
        let code = body
            .get("error")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("rejected");
        return Err(crate::runtime::executor_utils::failed_precondition_fields(
            format!("the OAuth provider refused the credential ({code}); store a new one"),
            [("secret_path", "store a new credential")],
        ));
    }
    if !status.is_success() {
        return Err(oauth_unavailable(format!(
            "the OAuth provider answered {status}"
        )));
    }
    Ok(body)
}

#[cfg(not(feature = "http-client"))]
async fn fetch_token(_credential: &OAuthCredential) -> Result<serde_json::Value, Status> {
    Err(super::errors::vault_capability_status(
        "lease_oauth_token",
        "http-client",
        "this broker was built without the http-client feature",
    ))
}

fn inner_request<T>(metadata: &tonic::metadata::MetadataMap, message: T) -> Request<T> {
    let mut request = Request::new(message);
    *request.metadata_mut() = metadata.clone();
    request
}

async fn read_credential(
    svc: &VaultServiceImpl,
    metadata: &tonic::metadata::MetadataMap,
    tenant_id: &str,
    secret_path: &str,
) -> Result<(OAuthCredential, i32), Status> {
    let secret = get_secret(
        svc,
        inner_request(
            metadata,
            vault_pb::GetSecretRequest {
                tenant_id: tenant_id.to_string(),
                secret_path: secret_path.to_string(),
                version: 0,
            },
        ),
    )
    .await?
    .into_inner();
    Ok((
        OAuthCredential::parse(&secret.secret_value)?,
        secret.version,
    ))
}

pub(crate) async fn lease_oauth_token(
    svc: &VaultServiceImpl,
    request: Request<vault_pb::LeaseOAuthTokenRequest>,
) -> Result<Response<vault_pb::LeaseOAuthTokenResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    let tenant_id = req.tenant_id.trim().to_string();
    let secret_path = req.secret_path.trim().to_string();
    if secret_path.is_empty() {
        return Err(vault_required_secret_path());
    }
    let min_valid = match i64::from(req.min_valid_seconds) {
        seconds if seconds > 0 => seconds,
        _ => DEFAULT_MIN_VALID_SECONDS,
    };
    let answer = |credential: &OAuthCredential, version: i32, refreshed: bool| {
        Response::new(vault_pb::LeaseOAuthTokenResponse {
            secret_path: secret_path.clone(),
            access_token: credential.access_token.clone(),
            expires_at_unix: credential.expires_at_unix,
            version,
            refreshed,
            message: "ok".to_string(),
            error: None,
        })
    };

    // Seal, tenant and scope checks run inside the read.
    let (credential, version) = read_credential(svc, &metadata, &tenant_id, &secret_path).await?;
    if credential.usable(now_unix(), min_valid) {
        return Ok(answer(&credential, version, false));
    }

    let runtime = svc.require_runtime()?;
    let pool = runtime.pg_pool()?;
    let mut lock = pool
        .acquire()
        .await
        .map_err(|_| oauth_unavailable("the vault database could not be reached"))?;
    let lock_key = format!("udb.vault.oauth:{tenant_id}:{secret_path}");
    let deadline = std::time::Instant::now() + LOCK_WAIT;
    loop {
        let acquired: bool =
            sqlx::query_scalar("SELECT pg_try_advisory_lock(hashtextextended($1, 0))")
                .bind(&lock_key)
                .fetch_one(&mut *lock)
                .await
                .map_err(|_| oauth_unavailable("the vault database could not be reached"))?;
        if acquired {
            break;
        }
        if std::time::Instant::now() >= deadline {
            return Err(oauth_unavailable(
                "another caller is refreshing this OAuth token; try again",
            ));
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    let refreshed = async {
        // Another caller may have refreshed while this one waited.
        let (mut credential, version) =
            read_credential(svc, &metadata, &tenant_id, &secret_path).await?;
        if credential.usable(now_unix(), min_valid) {
            return Ok(answer(&credential, version, false));
        }
        validate_token_url(&credential.token_url)?;
        let body = fetch_token(&credential).await?;
        credential.apply_token_response(&body, now_unix())?;
        let value = serde_json::to_string(&credential).map_err(|_| {
            vault_internal_status(
                "lease_oauth_token_store",
                "the OAuth credential could not be stored",
            )
        })?;
        let stored = put_secret(
            svc,
            inner_request(
                &metadata,
                vault_pb::PutSecretRequest {
                    tenant_id: tenant_id.clone(),
                    secret_path: secret_path.clone(),
                    secret_value: value,
                    expected_version: version,
                    ..Default::default()
                },
            ),
        )
        .await?
        .into_inner();
        Ok(answer(&credential, stored.version, true))
    }
    .await;

    let _ = sqlx::query("SELECT pg_advisory_unlock(hashtextextended($1, 0))")
        .bind(&lock_key)
        .execute(&mut *lock)
        .await;
    refreshed
}

#[cfg(test)]
mod tests {
    use super::*;

    fn credential(json: serde_json::Value) -> OAuthCredential {
        OAuthCredential::parse(&json.to_string()).expect("a credential")
    }

    #[test]
    fn a_secret_that_is_no_credential_is_refused() {
        assert!(OAuthCredential::parse("plain text").is_err());
        assert!(OAuthCredential::parse(r#"{"token_url":"https://x"}"#).is_err());
    }

    #[test]
    fn a_token_is_usable_only_with_a_known_expiry_inside_the_margin() {
        let mut c = credential(serde_json::json!({"token_url":"u","client_id":"c"}));
        assert!(!c.usable(100, 60));
        c.access_token = "t".to_string();
        assert!(!c.usable(100, 60), "no stated expiry");
        c.expires_at_unix = 150;
        assert!(!c.usable(100, 60));
        c.expires_at_unix = 160;
        assert!(c.usable(100, 60));
    }

    #[test]
    fn the_grant_follows_what_the_credential_holds() {
        let plain =
            credential(serde_json::json!({"token_url":"u","client_id":"c","client_secret":"s"}));
        assert!(
            plain
                .grant_form()
                .contains(&("grant_type", "client_credentials".to_string()))
        );
        let refresh =
            credential(serde_json::json!({"token_url":"u","client_id":"c","refresh_token":"r"}));
        let form = refresh.grant_form();
        assert!(form.contains(&("grant_type", "refresh_token".to_string())));
        assert!(form.contains(&("refresh_token", "r".to_string())));
    }

    #[test]
    fn a_rotated_refresh_token_replaces_the_old_and_an_unrotated_one_stays() {
        let mut c =
            credential(serde_json::json!({"token_url":"u","client_id":"c","refresh_token":"old"}));
        c.apply_token_response(
            &serde_json::json!({"access_token":"a","expires_in":"30"}),
            10,
        )
        .unwrap();
        assert_eq!(c.refresh_token, "old");
        assert_eq!(c.expires_at_unix, 40);
        c.apply_token_response(
            &serde_json::json!({"access_token":"b","expires_in":30,"refresh_token":"new"}),
            10,
        )
        .unwrap();
        assert_eq!(c.refresh_token, "new");
        assert!(c.apply_token_response(&serde_json::json!({}), 10).is_err());
    }
}
