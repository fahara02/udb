//! Login and the token lifecycle.
//!
//! The broker's refresh token is SINGLE-USE: every successful refresh mints a new
//! one and invalidates the presented one atomically. A client that keeps sending
//! its original refresh token authenticates, refreshes once, and then fails with
//! `Unauthenticated: invalid credential` at the second refresh boundary — an hour
//! or a day later, far from the code that caused it. [`TokenManager`] persists the
//! rotated token, which is the whole reason it exists rather than leaving refresh
//! to callers.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::Mutex;
use tonic::transport::Channel;
use tonic::Status;

use crate::metadata::Metadata;
use crate::proto::udb::core::authn::services::v1 as authn;
use crate::proto::udb::core::authn::services::v1::authn_service_client::AuthnServiceClient;

/// A stored credential set. `expires_at_unix` of 0 means "unknown"; the manager
/// then treats the token as valid until an explicit refresh.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Token {
    pub access_token: String,
    pub refresh_token: String,
    pub session_id: String,
    pub expires_at_unix: u64,
}

impl Token {
    /// Non-empty and not within `skew` of expiry.
    pub fn is_valid(&self, now_unix: u64, skew: Duration) -> bool {
        if self.access_token.is_empty() {
            return false;
        }
        if self.expires_at_unix == 0 {
            return true;
        }
        now_unix.saturating_add(skew.as_secs()) < self.expires_at_unix
    }
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Logs in and keeps the token fresh.
///
/// Refresh is single-flighted: concurrent callers that all observe a stale token
/// share one `RefreshToken` round-trip. Without that, N tasks would each present
/// the same single-use refresh token, one would win, and the rest would be told
/// their credential is invalid.
#[derive(Clone)]
pub struct TokenManager {
    inner: Arc<Inner>,
}

struct Inner {
    client: Mutex<AuthnServiceClient<Channel>>,
    token: Mutex<Token>,
    skew: Duration,
    meta: Metadata,
}

/// Hand-written rather than derived: a derived `Debug` would print the stored
/// access and refresh tokens, and this type is exactly the sort of thing that
/// ends up in a `tracing` field or a panic message.
impl std::fmt::Debug for TokenManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TokenManager")
            .field("tenant_id", &self.inner.meta.tenant_id)
            .field("project_id", &self.inner.meta.project_id)
            .field("skew", &self.inner.skew)
            .field("token", &"<redacted>")
            .finish()
    }
}

impl TokenManager {
    /// Default refresh skew: refresh this long before actual expiry.
    pub const DEFAULT_SKEW: Duration = Duration::from_secs(30);

    pub fn new(channel: Channel, meta: Metadata) -> Self {
        Self::with_skew(channel, meta, Self::DEFAULT_SKEW)
    }

    pub fn with_skew(channel: Channel, meta: Metadata, skew: Duration) -> Self {
        Self {
            inner: Arc::new(Inner {
                client: Mutex::new(AuthnServiceClient::new(channel)),
                token: Mutex::new(Token::default()),
                skew,
                meta,
            }),
        }
    }

    /// The currently stored token, without refreshing.
    pub async fn peek(&self) -> Token {
        self.inner.token.lock().await.clone()
    }

    /// Authenticate with username and password, storing the result.
    pub async fn login(
        &self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Result<Token, Status> {
        let mut req = tonic::Request::new(authn::LoginRequest {
            username: username.into(),
            password: password.into(),
            ..Default::default()
        });
        self.inner.meta.apply(&mut req)?;

        let resp = {
            let mut client = self.inner.client.lock().await;
            client.login(req).await?.into_inner()
        };

        if resp.mfa_required {
            return Err(Status::unauthenticated(
                "login requires a second factor; re-call Login with the MFA credential",
            ));
        }

        let token = Token {
            access_token: resp.access_token,
            refresh_token: resp.refresh_token,
            session_id: resp.session_id,
            expires_at_unix: expiry_from(resp.access_token_expires_in),
        };
        *self.inner.token.lock().await = token.clone();
        Ok(token)
    }

    /// The access token, refreshing first if it is stale.
    pub async fn access_token(&self) -> Result<String, Status> {
        {
            let token = self.inner.token.lock().await;
            if token.is_valid(now_unix(), self.inner.skew) {
                return Ok(token.access_token.clone());
            }
        }
        self.refresh().await.map(|t| t.access_token)
    }

    /// Force a refresh. Holding the token lock across the RPC is what makes this
    /// single-flighted: a second caller waits and then observes the new token
    /// rather than presenting the spent one.
    pub async fn refresh(&self) -> Result<Token, Status> {
        let mut stored = self.inner.token.lock().await;

        // Someone refreshed while we waited for the lock.
        if stored.is_valid(now_unix(), self.inner.skew) {
            return Ok(stored.clone());
        }
        if stored.refresh_token.is_empty() && stored.session_id.is_empty() {
            return Err(Status::unauthenticated(
                "no refresh token or session id stored; call login() first",
            ));
        }

        let mut req = tonic::Request::new(authn::RefreshTokenRequest {
            refresh_token: stored.refresh_token.clone(),
            session_id: stored.session_id.clone(),
        });
        self.inner.meta.apply(&mut req)?;

        let resp = {
            let mut client = self.inner.client.lock().await;
            client.refresh_token(req).await?.into_inner()
        };

        stored.access_token = resp.access_token;
        stored.expires_at_unix = expiry_from(resp.access_token_expires_in);
        // Persist the ROTATED refresh token. Guarded on non-empty because the
        // response omits it when the caller refreshed with a legacy server-side
        // session id rather than a token-family credential; assigning blindly
        // would erase a working credential.
        if !resp.refresh_token.is_empty() {
            stored.refresh_token = resp.refresh_token;
        }
        Ok(stored.clone())
    }

    /// Metadata carrying the current access token, ready for a data-plane client.
    pub async fn authenticated_metadata(&self) -> Result<Metadata, Status> {
        let token = self.access_token().await?;
        Ok(self.inner.meta.clone().with_bearer_token(token))
    }
}

fn expiry_from(expires_in_secs: i32) -> u64 {
    if expires_in_secs <= 0 {
        return 0;
    }
    now_unix().saturating_add(expires_in_secs as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_access_token_is_never_valid() {
        assert!(!Token::default().is_valid(1_000, Duration::from_secs(30)));
    }

    #[test]
    fn unknown_expiry_is_treated_as_valid() {
        let t = Token {
            access_token: "a".into(),
            expires_at_unix: 0,
            ..Default::default()
        };
        assert!(t.is_valid(u64::MAX, Duration::from_secs(30)));
    }

    #[test]
    fn skew_expires_the_token_early() {
        let t = Token {
            access_token: "a".into(),
            expires_at_unix: 1_000,
            ..Default::default()
        };
        assert!(t.is_valid(900, Duration::from_secs(30)), "930 < 1000");
        assert!(
            !t.is_valid(980, Duration::from_secs(30)),
            "1010 >= 1000: inside the skew window, must refresh"
        );
    }

    #[test]
    fn expiry_from_ignores_non_positive() {
        assert_eq!(expiry_from(0), 0);
        assert_eq!(expiry_from(-5), 0);
        assert!(expiry_from(60) >= now_unix());
    }
}
