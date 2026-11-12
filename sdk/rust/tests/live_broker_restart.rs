//! Live broker-restart recovery: a Rust client outlives a broker restart
//! without being recreated. The same `UdbClient` (its bearer owned by a
//! `TokenManager`) and the same native channel reconnect and keep working on the
//! data plane and the native control plane. Same contract as the Go SDK's
//! `TestLiveBrokerRestartRecovery`.
//!
//! Runs when `UDB_LIVE_SDK_TESTS=1` and `UDB_LIVE_BROKER_RESTART_CMD` names a
//! command (run with `sh -c`) that restarts the broker on the same addresses and
//! KEEPS its database (sessions, users and tenant ids survive). While the broker
//! is down a call may fail UNAVAILABLE; any other failure, or no recovery within
//! 90s, fails the test. Otherwise the test passes without touching the network.
//!
//! Run: `cargo test --test live_broker_restart -- --nocapture`

use std::time::{Duration, Instant};

use tonic::transport::Endpoint;
use tonic::{Code, Request, Status};
use udb_client::proto::udb::core::authn::entity::v1::TokenType;
use udb_client::proto::udb::core::authn::services::v1 as authn;
use udb_client::proto::udb::core::authn::services::v1::authn_service_client::AuthnServiceClient;
use udb_client::proto::udb::entity::v1::CapabilitiesRequest;
use udb_client::{Metadata, TokenManager, UdbClient};

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
    request.set_timeout(Duration::from_secs(5));
    request
}

/// Data plane: GetCapabilities through the SDK client that owns its bearer.
async fn data_probe(data: &UdbClient) -> Result<(), Code> {
    let mut data = data.clone();
    data.get_capabilities(CapabilitiesRequest::default())
        .await
        .map(|_| ())
        .map_err(|err| err.code())
}

/// Native control plane: ValidateSession (ValidateToken on the owned bearer).
async fn native_probe(
    native: &AuthnServiceClient<tonic::transport::Channel>,
    tokens: &TokenManager,
    tenant: &str,
) -> Result<(), Code> {
    let mut meta = tokens
        .authenticated_metadata()
        .await
        .map_err(|s| s.code())?;
    meta.tenant_id = tenant.to_string();
    let token = meta.bearer_token.clone();
    let validated = native
        .clone()
        .validate_token(request(
            &meta,
            authn::ValidateTokenRequest {
                token,
                token_type: TokenType::JwtAccess as i32,
            },
        ))
        .await
        .map_err(|status: Status| status.code())?
        .into_inner();
    assert!(validated.valid, "the session token is not valid");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn live_broker_restart_recovery() {
    let restart = std::env::var("UDB_LIVE_BROKER_RESTART_CMD").unwrap_or_default();
    if std::env::var("UDB_LIVE_SDK_TESTS").as_deref() != Ok("1") || restart.trim().is_empty() {
        eprintln!("skipped: needs UDB_LIVE_SDK_TESTS=1 and UDB_LIVE_BROKER_RESTART_CMD");
        return;
    }
    let auth = Endpoint::from_shared(endpoint(&required("UDB_AUTH_GRPC_TARGET")))
        .expect("endpoint")
        .connect_timeout(Duration::from_secs(10))
        .connect()
        .await
        .expect("connect the native listener");
    let mut base =
        Metadata::new(required("UDB_LIVE_TENANT")).with_project(required("UDB_LIVE_PROJECT"));
    base.purpose = "rust.live.broker-restart".into();
    base.correlation_id = "rust-live-broker-restart".into();
    let tokens = TokenManager::new(auth.clone(), base.clone());
    let login = tokens
        .login(required("UDB_LIVE_USERNAME"), required("UDB_LIVE_PASSWORD"))
        .await
        .expect("login");
    let native = AuthnServiceClient::new(auth);
    // Authenticated calls assert the canonical tenant the login carries.
    let tenant = native
        .clone()
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
        .expect("authenticate")
        .into_inner()
        .principal
        .expect("principal")
        .tenant_id;
    let mut data_meta = base.clone();
    data_meta.tenant_id = tenant.clone();
    let data = UdbClient::connect(endpoint(&required("UDB_GRPC_TARGET")), data_meta)
        .await
        .expect("connect the data listener")
        .with_token_manager(tokens.clone());

    data_probe(&data)
        .await
        .unwrap_or_else(|code| panic!("before restart, data GetCapabilities: code={code:?}"));
    native_probe(&native, &tokens, &tenant)
        .await
        .unwrap_or_else(|code| panic!("before restart, native ValidateSession: code={code:?}"));

    let status = std::process::Command::new("sh")
        .arg("-c")
        .arg(&restart)
        .status()
        .expect("run UDB_LIVE_BROKER_RESTART_CMD");
    assert!(
        status.success(),
        "UDB_LIVE_BROKER_RESTART_CMD failed: {status}"
    );

    for name in ["data GetCapabilities", "native ValidateSession"] {
        let deadline = Instant::now() + Duration::from_secs(90);
        loop {
            let result = if name.starts_with("data") {
                data_probe(&data).await
            } else {
                native_probe(&native, &tokens, &tenant).await
            };
            match result {
                Ok(()) => break,
                Err(Code::Unavailable) if Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
                Err(Code::Unavailable) => {
                    panic!("after restart, {name} stayed UNAVAILABLE for 90s")
                }
                Err(code) => panic!("after restart, {name} on the same client: code={code:?}"),
            }
        }
    }
}
