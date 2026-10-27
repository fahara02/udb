//! Live refresh-token rotation conformance through the Rust SDK's `TokenManager`.
//!
//! The broker rotates the refresh token on every `AuthnService.RefreshToken`
//! call and each one works once. An SDK that kept the token it logged in with
//! refreshed once and then failed with UNAUTHENTICATED "invalid credential" — a
//! production outage no single-refresh test could catch. This drives the SDK's
//! own refresh path several times in a row.
//!
//! Gated on `UDB_LIVE_SDK_TESTS=1`; without it the test passes without touching
//! the network. Needs `UDB_AUTH_GRPC_TARGET` (the native listener that serves
//! AuthnService), `UDB_LIVE_USERNAME`, `UDB_LIVE_PASSWORD`, `UDB_LIVE_TENANT`,
//! and optionally `UDB_LIVE_PROJECT`. CI's "SDK refresh-token rotation (live)"
//! job runs it against the broker it just built, on every push to main.
//!
//! Run: `cargo test --test live_refresh_rotation -- --nocapture`

use std::time::Duration;

use tonic::transport::Endpoint;
use tonic::{Code, Request};
use udb_client::proto::udb::core::authn::entity::v1::TokenType;
use udb_client::proto::udb::core::authn::services::v1 as authn;
use udb_client::proto::udb::core::authn::services::v1::authn_service_client::AuthnServiceClient;
use udb_client::{Metadata, TokenManager};

const REFRESHES: usize = 3;

/// Longer than any access-token lifetime, so the manager treats every stored
/// token as stale and each `authenticated_metadata()` call performs a real
/// refresh — the same path a long-running consumer takes at each expiry.
const ALWAYS_STALE: Duration = Duration::from_secs(365 * 24 * 60 * 60);

fn required(name: &str) -> String {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| panic!("{name} is required when UDB_LIVE_SDK_TESTS=1"))
}

fn endpoint(address: &str) -> String {
    if address.contains("://") {
        address.to_string()
    } else {
        format!("http://{address}")
    }
}

fn request<T>(meta: &Metadata, message: T) -> Request<T> {
    let mut request = Request::new(message);
    meta.apply(&mut request).expect("metadata");
    request
}

#[tokio::test(flavor = "multi_thread")]
async fn live_refresh_token_rotation_survives_repeated_refreshes() {
    if std::env::var("UDB_LIVE_SDK_TESTS").as_deref() != Ok("1") {
        eprintln!("skipped: the live refresh-rotation test needs UDB_LIVE_SDK_TESTS=1");
        return;
    }
    let channel = Endpoint::from_shared(endpoint(&required("UDB_AUTH_GRPC_TARGET")))
        .expect("endpoint")
        .connect_timeout(Duration::from_secs(10))
        .connect()
        .await
        .expect("connect the native listener");
    let mut base = Metadata::new(required("UDB_LIVE_TENANT"))
        .with_project(std::env::var("UDB_LIVE_PROJECT").unwrap_or_else(|_| "default".into()));
    base.purpose = "rust.live.refresh-rotation".into();

    let tokens = TokenManager::with_skew(channel.clone(), base.clone(), ALWAYS_STALE);
    let login = tokens
        .login(required("UDB_LIVE_USERNAME"), required("UDB_LIVE_PASSWORD"))
        .await
        .expect("login");
    assert!(
        !login.access_token.is_empty() && !login.refresh_token.is_empty(),
        "login must store an access and a refresh token"
    );
    let first = login.refresh_token.clone();

    // The login JWT carries the canonical tenant UUID, not the human code;
    // authenticated calls must assert that tenant.
    let mut authn_client = AuthnServiceClient::new(channel);
    let principal = authn_client
        .authenticate(request(
            &base,
            authn::AuthnRequest {
                bearer_token: login.access_token.clone(),
                tenant_hint: base.tenant_id.clone(),
                project_hint: base.project_id.clone(),
                ..Default::default()
            },
        ))
        .await
        .expect("authenticate the login token")
        .into_inner()
        .principal
        .expect("principal");
    assert!(
        !principal.tenant_id.is_empty(),
        "the verified principal must carry its canonical tenant"
    );

    let mut previous = first.clone();
    for i in 1..=REFRESHES {
        let mut authed = tokens
            .authenticated_metadata()
            .await
            .unwrap_or_else(|status| panic!("refresh {i} through TokenManager failed: {status}"));
        let stored = tokens.peek().await;
        assert!(
            !stored.access_token.is_empty() && !stored.refresh_token.is_empty(),
            "refresh {i} stored an empty access or refresh token"
        );
        assert_ne!(
            stored.refresh_token, previous,
            "refresh {i}: the refresh token did not change — the broker stopped rotating it or the SDK kept the spent one"
        );
        assert_eq!(
            authed.bearer_token, stored.access_token,
            "refresh {i}: the metadata does not carry the refreshed bearer"
        );

        // An authenticated call carrying the refreshed bearer.
        authed.tenant_id = principal.tenant_id.clone();
        if !principal.project_id.is_empty() {
            authed.project_id = principal.project_id.clone();
        }
        let validated = authn_client
            .validate_token(request(
                &authed,
                authn::ValidateTokenRequest {
                    token: stored.access_token.clone(),
                    token_type: TokenType::JwtAccess as i32,
                },
            ))
            .await
            .unwrap_or_else(|status| panic!("refresh {i}: ValidateToken failed: {status}"))
            .into_inner();
        assert!(
            validated.valid,
            "refresh {i}: the refreshed access token does not validate"
        );
        previous = stored.refresh_token;
    }

    // The first refresh token was spent three rotations ago: the broker must
    // refuse it. (Replaying the immediately-previous token instead would trip
    // reuse detection and revoke every session of this shared live user.)
    let refused = authn_client
        .refresh_token(request(
            &base,
            authn::RefreshTokenRequest {
                refresh_token: first,
                session_id: String::new(),
            },
        ))
        .await
        .expect_err("a spent refresh token was accepted");
    assert_eq!(
        refused.code(),
        Code::Unauthenticated,
        "a spent refresh token must be refused with UNAUTHENTICATED: {refused}"
    );
}
