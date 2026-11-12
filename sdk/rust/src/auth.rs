//! Login and the token lifecycle.
//!
//! The broker's refresh token is SINGLE-USE: every successful refresh mints a new
//! one and invalidates the presented one atomically. A client that keeps sending
//! its original refresh token authenticates, refreshes once, and then fails with
//! `Unauthenticated: invalid credential` at the second refresh boundary — an hour
//! or a day later, far from the code that caused it. [`TokenManager`] persists the
//! rotated token, which is the whole reason it exists rather than leaving refresh
//! to callers.
//!
//! # Recovery after a broker restart
//!
//! A client must outlive the broker it talks to. [`TokenManager::login`] retries
//! a transient failure (broker unreachable or restarting, overloaded, reply
//! lost), so a service that starts while the broker restarts still signs in.
//! When the broker refuses the access token the manager issued before its local
//! expiry (a restarted broker that lost the session, a revoked session),
//! [`TokenManager::reject`] marks it, and the next [`TokenManager::access_token`]
//! renews it: refresh, else sign in again. A [`crate::UdbClient`] built with
//! [`crate::UdbClient::with_token_manager`] does this itself and repeats the
//! refused call exactly once. A token renewed moments ago that is refused again
//! is not renewed again, and while a refused token cannot be renewed, calls are
//! refused locally with a backoff (1s doubling to 60s) instead of one sign-in
//! per call.

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::sync::Mutex;
use tonic::transport::Channel;
use tonic::{Code, Status};

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

/// Sign-in attempts [`TokenManager::login`] makes before it gives up on a
/// transient failure.
pub const LOGIN_ATTEMPTS: u32 = 3;

/// First wait after a failed attempt; doubles per consecutive failure.
const RENEW_BACKOFF_BASE: Duration = Duration::from_secs(1);
/// Longest wait between attempts.
const RENEW_BACKOFF_CAP: Duration = Duration::from_secs(60);
/// How soon after renewing a refused token another refusal is taken as final
/// rather than as a reason to renew again.
const REJECTED_RENEW_INTERVAL: Duration = Duration::from_secs(5);

/// Whether repeating a failed sign-in can help: the broker was unreachable,
/// overloaded, or the reply was lost. A refused credential is final.
fn transient(status: &Status) -> bool {
    matches!(
        status.code(),
        Code::Unavailable | Code::DeadlineExceeded | Code::ResourceExhausted | Code::Aborted
    )
}

/// The wait after the n-th consecutive failure: a random time in
/// `[ceiling/2, ceiling]`, where `ceiling = base * 2^(n-1)`, capped at `limit`.
fn jittered_backoff(failures: u32, base: Duration, limit: Duration) -> Duration {
    let mut ceiling = base;
    let mut i = 1;
    while i < failures && ceiling < limit {
        ceiling *= 2;
        i += 1;
    }
    let ceiling = ceiling.min(limit);
    let half = ceiling / 2;
    let span = u64::try_from((ceiling - half).as_millis()).unwrap_or(u64::MAX);
    // A freshly keyed SipHash of nothing: random enough for jitter, with no
    // dependency on a random-number crate.
    let random = RandomState::new().build_hasher().finish();
    half + Duration::from_millis(random % span.saturating_add(1))
}

/// What the manager knows about tokens the broker refused before their expiry.
#[derive(Default)]
struct RefusalState {
    /// The refused access token; a stored token carrying it is renewed as if
    /// it were stale.
    rejected: String,
    /// When `rejected` was marked.
    rejected_at: Option<Instant>,
    /// Consecutive failed renewals of the refused token.
    failures: u32,
    /// Until when calls are refused locally after a failed renewal, and with
    /// what.
    retry_at: Option<Instant>,
    last_error: Option<(Code, String)>,
}

impl RefusalState {
    /// Whether the broker's refusal of `sent` should be answered by renewing
    /// the token and repeating the call. `current` is the token the manager
    /// holds now.
    fn reject(&mut self, sent: &str, current: &str, now: Instant) -> bool {
        if sent.is_empty() {
            return false;
        }
        if sent == self.rejected {
            // Already marked by a concurrent call; the next resolve renews it.
            return true;
        }
        if sent != current {
            // Not this manager's token, or one already replaced by an
            // ordinary refresh: nothing to renew.
            return false;
        }
        if let Some(at) = self.rejected_at {
            if now.saturating_duration_since(at) < REJECTED_RENEW_INTERVAL {
                // The token renewed moments ago is refused too: renewing
                // cannot help.
                return false;
            }
        }
        self.rejected = sent.to_string();
        self.rejected_at = Some(now);
        self.renewed();
        true
    }

    fn is_rejected(&self, access_token: &str) -> bool {
        !self.rejected.is_empty() && self.rejected == access_token
    }

    /// The local refusal while a failed renewal backs off.
    fn refused_until(&self, now: Instant) -> Option<Status> {
        match (self.retry_at, &self.last_error) {
            (Some(at), Some((code, message))) if now < at => Some(Status::new(
                *code,
                format!("credential refused by the broker and not yet renewed: {message}"),
            )),
            _ => None,
        }
    }

    fn renew_failed(&mut self, err: &Status, now: Instant) {
        self.failures = self.failures.saturating_add(1);
        self.retry_at =
            Some(now + jittered_backoff(self.failures, RENEW_BACKOFF_BASE, RENEW_BACKOFF_CAP));
        self.last_error = Some((err.code(), err.message().to_string()));
    }

    fn renewed(&mut self) {
        self.failures = 0;
        self.retry_at = None;
        self.last_error = None;
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
    /// The username and password of the last successful login, kept so a
    /// session the broker lost (a restart) can sign in again when its refresh
    /// token is refused too. Never printed.
    sign_in: std::sync::Mutex<Option<(String, String)>>,
    refusal: std::sync::Mutex<RefusalState>,
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
                sign_in: std::sync::Mutex::new(None),
                refusal: std::sync::Mutex::new(RefusalState::default()),
            }),
        }
    }

    // The two std mutexes below are never held across an await: every guard is
    // a temporary dropped within its statement.
    fn refusal(&self) -> std::sync::MutexGuard<'_, RefusalState> {
        self.inner
            .refusal
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn sign_in_credentials(&self) -> Option<(String, String)> {
        self.inner
            .sign_in
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    fn remember_sign_in(&self, username: String, password: String) {
        *self
            .inner
            .sign_in
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some((username, password));
    }

    /// The currently stored token, without refreshing.
    pub async fn peek(&self) -> Token {
        self.inner.token.lock().await.clone()
    }

    /// Authenticate with username and password, storing the result.
    ///
    /// A transient failure (broker unreachable or restarting, overloaded, reply
    /// lost) is retried, up to [`LOGIN_ATTEMPTS`] attempts in all with a
    /// jittered, doubling wait between them. A refused credential is final.
    /// The username and password are kept in memory so a session the broker
    /// later loses can sign in again (see [`TokenManager::reject`]).
    pub async fn login(
        &self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Result<Token, Status> {
        let username = username.into();
        let password = password.into();
        let mut last = None;
        for attempt in 0..LOGIN_ATTEMPTS {
            if attempt > 0 {
                let wait = jittered_backoff(attempt, RENEW_BACKOFF_BASE, RENEW_BACKOFF_CAP);
                tokio::time::sleep(wait).await;
            }
            match self.login_rpc(username.clone(), password.clone()).await {
                Ok(token) => {
                    *self.inner.token.lock().await = token.clone();
                    self.remember_sign_in(username, password);
                    self.refusal().renewed();
                    return Ok(token);
                }
                Err(err) if transient(&err) => last = Some(err),
                Err(err) => return Err(err),
            }
        }
        Err(last.unwrap_or_else(|| Status::unavailable("login was not attempted")))
    }

    /// One `Login` round-trip; the caller stores the result.
    async fn login_rpc(&self, username: String, password: String) -> Result<Token, Status> {
        let mut req = tonic::Request::new(authn::LoginRequest {
            username,
            password,
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

        Ok(Token {
            access_token: resp.access_token,
            refresh_token: resp.refresh_token,
            session_id: resp.session_id,
            expires_at_unix: expiry_from(resp.access_token_expires_in),
        })
    }

    /// The access token, refreshing first if it is stale or the broker refused it.
    pub async fn access_token(&self) -> Result<String, Status> {
        {
            let token = self.inner.token.lock().await;
            let rejected = self.refusal().is_rejected(&token.access_token);
            if !rejected && token.is_valid(now_unix(), self.inner.skew) {
                return Ok(token.access_token.clone());
            }
        }
        self.refresh().await.map(|t| t.access_token)
    }

    /// Force a refresh. Holding the token lock across the RPC is what makes this
    /// single-flighted: a second caller waits and then observes the new token
    /// rather than presenting the spent one.
    ///
    /// A token the broker refused (see [`TokenManager::reject`]) is renewed even
    /// though it has not expired: refresh, else sign in again with the stored
    /// login. While such a renewal keeps failing it is retried at most once per
    /// backoff interval (1s doubling to 60s); calls in between are refused
    /// locally with the last failure.
    pub async fn refresh(&self) -> Result<Token, Status> {
        let mut stored = self.inner.token.lock().await;
        let rejected = self.refusal().is_rejected(&stored.access_token);

        // Someone refreshed while we waited for the lock.
        if !rejected && stored.is_valid(now_unix(), self.inner.skew) {
            return Ok(stored.clone());
        }
        if !rejected {
            return self.refresh_rpc(&mut stored).await;
        }

        let refused = self.refusal().refused_until(Instant::now());
        if let Some(status) = refused {
            return Err(status);
        }
        let result = match self.refresh_rpc(&mut stored).await {
            Err(err) if err.code() == Code::Unauthenticated => {
                // The broker lost the session too (a restart): sign in again.
                match self.sign_in_credentials() {
                    Some((username, password)) => {
                        self.login_rpc(username, password).await.inspect(|token| {
                            *stored = token.clone();
                        })
                    }
                    None => Err(err),
                }
            }
            other => other,
        };
        match &result {
            Ok(_) => self.refusal().renewed(),
            Err(err) => self.refusal().renew_failed(err, Instant::now()),
        }
        result
    }

    /// One `RefreshToken` round-trip folded into `stored`.
    async fn refresh_rpc(&self, stored: &mut Token) -> Result<Token, Status> {
        if stored.refresh_token.is_empty() && stored.session_id.is_empty() {
            return Err(Status::unauthenticated(
                "no refresh token or session id stored; call login() first",
            ));
        }

        let mut req = tonic::Request::new(refresh_request(stored));
        self.inner.meta.apply(&mut req)?;

        let resp = {
            let mut client = self.inner.client.lock().await;
            client.refresh_token(req).await?.into_inner()
        };

        apply_refresh(stored, resp);
        Ok(stored.clone())
    }

    /// Report that the broker refused `access_token` (UNAUTHENTICATED) before
    /// its local expiry. Returns true when a repeat of the refused call can
    /// carry a different, renewed token: the token is this manager's current
    /// one (or was already marked by a concurrent call), and it was not itself
    /// renewed less than 5s ago. The next [`TokenManager::access_token`] then
    /// renews it.
    ///
    /// Only for a call that carried this manager's token and is not an
    /// `AuthnService` call (a refusal there is about the credential in the
    /// request, not the bearer). [`crate::UdbClient::with_token_manager`] calls
    /// it for you.
    pub async fn reject(&self, access_token: &str) -> bool {
        let current = self.inner.token.lock().await.access_token.clone();
        self.refusal()
            .reject(access_token, &current, Instant::now())
    }

    /// Metadata carrying the current access token, ready for a data-plane client.
    pub async fn authenticated_metadata(&self) -> Result<Metadata, Status> {
        let token = self.access_token().await?;
        Ok(self.inner.meta.clone().with_bearer_token(token))
    }
}

/// The `RefreshToken` request for the stored credential.
fn refresh_request(stored: &Token) -> authn::RefreshTokenRequest {
    authn::RefreshTokenRequest {
        refresh_token: stored.refresh_token.clone(),
        session_id: stored.session_id.clone(),
    }
}

/// Fold a `RefreshToken` reply into the stored credential.
fn apply_refresh(stored: &mut Token, resp: authn::RefreshTokenResponse) {
    stored.access_token = resp.access_token;
    stored.expires_at_unix = expiry_from(resp.access_token_expires_in);
    // Persist the ROTATED refresh token. Guarded on non-empty because the
    // response omits it when the caller refreshed with a legacy server-side
    // session id rather than a token-family credential; assigning blindly
    // would erase a working credential.
    if !resp.refresh_token.is_empty() {
        stored.refresh_token = resp.refresh_token;
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

    /// Stands in for the broker: it accepts only the refresh token it minted
    /// last and mints a new one on every refresh, so each token works once.
    /// `None` is the broker's "invalid credential".
    struct SingleUseIssuer {
        live: String,
        minted: u32,
        presented: Vec<String>,
    }

    impl SingleUseIssuer {
        fn new(first: &str) -> Self {
            Self {
                live: first.to_string(),
                minted: 1,
                presented: Vec::new(),
            }
        }

        fn refresh(
            &mut self,
            req: authn::RefreshTokenRequest,
        ) -> Option<authn::RefreshTokenResponse> {
            self.presented.push(req.refresh_token.clone());
            if req.refresh_token != self.live {
                return None;
            }
            self.minted += 1;
            self.live = format!("refresh-{}", self.minted);
            Some(authn::RefreshTokenResponse {
                access_token: format!("access-{}", self.minted),
                access_token_expires_in: 900,
                refresh_token: self.live.clone(),
                refresh_token_expires_in: 86_400,
                session_public_id: "public-session".into(),
            })
        }
    }

    // The production failure: keeping the login refresh token refreshed once and
    // then failed every later refresh. Each refresh must present the token the
    // previous one returned.
    #[test]
    fn repeated_refreshes_present_each_rotated_token() {
        let mut issuer = SingleUseIssuer::new("refresh-1");
        let mut stored = Token {
            access_token: "access-1".into(),
            refresh_token: "refresh-1".into(),
            session_id: "legacy-session".into(),
            expires_at_unix: 0,
        };

        for _ in 0..4 {
            let resp = issuer
                .refresh(refresh_request(&stored))
                .expect("the rotated refresh token must be accepted");
            apply_refresh(&mut stored, resp);
        }

        assert_eq!(
            issuer.presented,
            ["refresh-1", "refresh-2", "refresh-3", "refresh-4"]
        );
        assert_eq!(stored.refresh_token, "refresh-5");
        assert_eq!(stored.access_token, "access-5");
        assert_eq!(
            stored.session_id, "legacy-session",
            "the legacy refresh session id is kept"
        );
        assert!(stored.expires_at_unix >= now_unix());
    }

    #[test]
    fn legacy_session_refresh_without_a_rotated_token_keeps_the_stored_one() {
        let mut stored = Token {
            access_token: "access-1".into(),
            refresh_token: "refresh-1".into(),
            session_id: "legacy-session".into(),
            expires_at_unix: 0,
        };
        apply_refresh(
            &mut stored,
            authn::RefreshTokenResponse {
                access_token: "access-2".into(),
                access_token_expires_in: 900,
                ..Default::default()
            },
        );
        assert_eq!(stored.refresh_token, "refresh-1");
        assert_eq!(stored.access_token, "access-2");
    }

    // Guards the stand-in itself: without rotation the second refresh fails,
    // which is exactly what the test above proves the manager avoids.
    #[test]
    fn a_spent_refresh_token_is_refused_by_the_issuer() {
        let mut issuer = SingleUseIssuer::new("refresh-1");
        let spent = Token {
            refresh_token: "refresh-1".into(),
            ..Default::default()
        };
        assert!(issuer.refresh(refresh_request(&spent)).is_some());
        assert!(issuer.refresh(refresh_request(&spent)).is_none());
    }

    #[test]
    fn a_refusal_of_the_current_token_marks_it_for_renewal() {
        let now = Instant::now();
        let mut state = RefusalState::default();
        assert!(state.reject("access-1", "access-1", now));
        assert!(state.is_rejected("access-1"));
        assert!(!state.is_rejected("access-2"));
        // A concurrent call that carried the same token is repeated too.
        assert!(state.reject("access-1", "access-1", now));
    }

    // Negative: a token this manager does not hold now (a caller's own, or one
    // already replaced by an ordinary refresh) is never renewed.
    #[test]
    fn a_refusal_of_a_token_the_manager_does_not_hold_is_final() {
        let mut state = RefusalState::default();
        assert!(!state.reject("someone-else", "access-1", Instant::now()));
        assert!(!state.reject("", "", Instant::now()));
        assert!(!state.is_rejected("access-1"));
    }

    // Negative: a token renewed moments ago that is refused again is not
    // renewed again; renewing cannot help, and it must not become a storm.
    #[test]
    fn a_renewed_token_refused_within_five_seconds_is_final() {
        let t0 = Instant::now();
        let mut state = RefusalState::default();
        assert!(state.reject("access-1", "access-1", t0));
        state.renewed();
        for offset in [0, 1, 4] {
            assert!(
                !state.reject("access-2", "access-2", t0 + Duration::from_secs(offset)),
                "renewed {offset}s ago"
            );
        }
        assert!(state.reject("access-2", "access-2", t0 + Duration::from_secs(6)));
        assert!(state.is_rejected("access-2"));
    }

    #[test]
    fn a_failed_renewal_refuses_locally_with_a_growing_backoff() {
        let t0 = Instant::now();
        let mut state = RefusalState::default();
        assert!(state.reject("access-1", "access-1", t0));
        assert!(state.refused_until(t0).is_none(), "the first renewal runs");

        let refused = Status::unauthenticated("invalid credential");
        for failure in 1..=8u32 {
            state.renew_failed(&refused, t0);
            let wait = state.retry_at.expect("a backoff") - t0;
            let ceiling = (RENEW_BACKOFF_BASE * 2u32.pow(failure - 1)).min(RENEW_BACKOFF_CAP);
            assert!(
                wait >= ceiling / 2 && wait <= ceiling,
                "failure {failure}: waited {wait:?}, ceiling {ceiling:?}"
            );
            let local = state.refused_until(t0).expect("refused locally");
            assert_eq!(local.code(), Code::Unauthenticated);
        }
        assert!(state.refused_until(t0 + RENEW_BACKOFF_CAP).is_none());
        state.renewed();
        assert!(state.refused_until(t0).is_none());
    }

    #[test]
    fn only_unreachable_overloaded_or_lost_reply_codes_are_transient() {
        for code in [
            Code::Unavailable,
            Code::DeadlineExceeded,
            Code::ResourceExhausted,
            Code::Aborted,
        ] {
            assert!(transient(&Status::new(code, "")), "{code:?}");
        }
        for code in [
            Code::Unauthenticated,
            Code::PermissionDenied,
            Code::InvalidArgument,
            Code::Internal,
        ] {
            assert!(!transient(&Status::new(code, "")), "{code:?}");
        }
    }

    #[test]
    fn jittered_backoff_stays_within_its_ceiling() {
        for _ in 0..50 {
            let first = jittered_backoff(1, RENEW_BACKOFF_BASE, RENEW_BACKOFF_CAP);
            assert!(first >= Duration::from_millis(500) && first <= RENEW_BACKOFF_BASE);
            let second = jittered_backoff(2, RENEW_BACKOFF_BASE, RENEW_BACKOFF_CAP);
            assert!(second >= RENEW_BACKOFF_BASE && second <= Duration::from_secs(2));
            let capped = jittered_backoff(30, RENEW_BACKOFF_BASE, RENEW_BACKOFF_CAP);
            assert!(capped >= RENEW_BACKOFF_CAP / 2 && capped <= RENEW_BACKOFF_CAP);
        }
    }

    #[test]
    fn expiry_from_ignores_non_positive() {
        assert_eq!(expiry_from(0), 0);
        assert_eq!(expiry_from(-5), 0);
        assert!(expiry_from(60) >= now_unix());
    }
}
