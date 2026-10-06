//! Service-caller conformance over the SERVED native control plane.
//!
//! A service that exchanged its API key for a bearer (`AuthnService.Authenticate`)
//! was denied on native methods three times in a row, each by a different gate
//! (credential type, then request context, then scope), and no test drove native
//! RPCs through the served stack as a service account. This test does: it mounts
//! the native plane through the SAME function production uses
//! (`NativeControlPlaneServices::into_routes`), behind the credential layer and
//! the proto-driven method-security layer, and calls every method whose
//! `endpoint_security` admits a service account or API key — once with the
//! exchanged bearer and once with the raw `x-api-key`. Any denial raised by the
//! security gates fails the test; business errors from the handler (empty request,
//! missing backend) pass, because they prove the call got through the gates.

use super::support::*;
use crate::proto::udb::core::apikey::services::v1 as apikey_pb;
use crate::proto::udb::core::apikey::services::v1::api_key_service_server::ApiKeyService;
use crate::proto::udb::core::authn::entity::v1 as authn_entity_pb;
use crate::proto::udb::core::authn::services::v1 as authn_pb;
use crate::proto::udb::core::authn::services::v1::authn_service_server::AuthnService;
use crate::proto::udb::core::common::v1 as common_pb;
use crate::runtime::service::method_security::{
    AuthMode, MethodSecurity, MethodSecurityLayer, all_native_service_rpc_paths, method_security,
    scope_claim_context_for_test, test_claim_context,
};
use futures::StreamExt as _;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;
use tonic::Request;
use tonic::codegen::http::uri::PathAndQuery;

/// Mirror of `udb.core.common.v1.CredentialType` values this test selects on.
const CREDENTIAL_TYPE_API_KEY: i32 = 3;
const CREDENTIAL_TYPE_SERVICE_ACCOUNT: i32 = 4;

/// Ratchet: the number of native RPCs a service account may call (non-public,
/// not `internal_grpc_only`, `allowed_credential_types` lists
/// CREDENTIAL_TYPE_SERVICE_ACCOUNT or CREDENTIAL_TYPE_API_KEY). Lowering it means
/// a method silently stopped admitting services. When you deliberately ADD
/// service-callable methods, raise it to the new count; recount with:
///
/// ```text
/// python - <<'EOF'
/// import re, glob
/// n = 0
/// for f in glob.glob("proto/udb/core/**/services/**/*.proto", recursive=True):
///     for e in re.findall(r"endpoint_security\)\s*=\s*\{(.*?)\};", open(f).read(), re.S):
///         if ("CREDENTIAL_TYPE_SERVICE_ACCOUNT" in e or "CREDENTIAL_TYPE_API_KEY" in e) \
///            and not re.search(r"internal_grpc_only:\s*true", e) and "AUTH_MODE_PUBLIC" not in e:
///             n += 1
/// print(n)
/// EOF
/// ```
const SERVICE_CALLABLE_PATH_FLOOR: usize = 130;

/// How long to wait for the first response message once headers arrived. An
/// open server/bidi stream that yields nothing has already passed every gate.
const FIRST_MESSAGE_WAIT: Duration = Duration::from_secs(2);
/// Upper bound for a whole call. Method security answers before the handler
/// runs, so a call that is still pending here has passed the gates too.
const CALL_TIMEOUT: Duration = Duration::from_secs(15);
const CONCURRENT_CALLS: usize = 16;

fn is_service_callable(security: &MethodSecurity) -> bool {
    security.mode != AuthMode::Public
        && !security.internal_grpc_only
        && security.allowed_credential_types.iter().any(|kind| {
            *kind == CREDENTIAL_TYPE_SERVICE_ACCOUNT || *kind == CREDENTIAL_TYPE_API_KEY
        })
}

/// Every native RPC path a service account may call, sorted.
fn service_callable_paths() -> Vec<(String, &'static MethodSecurity)> {
    let mut paths: Vec<(String, &'static MethodSecurity)> = all_native_service_rpc_paths()
        .into_iter()
        .filter_map(|path| {
            let security = method_security(&path)?;
            is_service_callable(security).then_some((path, security))
        })
        .collect();
    paths.sort_by(|left, right| left.0.cmp(&right.0));
    paths.dedup_by(|left, right| left.0 == right.0);
    paths
}

#[derive(Clone, Copy)]
enum Credential<'a> {
    Bearer(&'a str),
    ApiKey(&'a str),
}

impl Credential<'_> {
    fn label(&self) -> &'static str {
        match self {
            Credential::Bearer(_) => "bearer",
            Credential::ApiKey(_) => "x-api-key",
        }
    }
}

enum CallOutcome {
    /// The handler answered (any response message or a clean end of stream).
    Answered,
    /// The call is still pending (open stream / slow handler) — past the gates.
    Pending,
    Failed(tonic::Status),
    /// The test client could not reach the listener at all.
    Transport(String),
}

/// Issue one gRPC call to `path` with an EMPTY request message. Every protobuf
/// message decodes from an empty body (all fields default), and the response is
/// decoded as `()` (google.protobuf.Empty), which skips every unknown field, so one codec fits every
/// method. `streaming` covers unary, server-, client- and bidi-streaming
/// methods alike: one request message is sent, then the request stream ends.
async fn call_native(
    channel: tonic::transport::Channel,
    path: &str,
    credential: Credential<'_>,
    tenant_id: &str,
    project_id: &str,
    request_id: Option<&str>,
) -> CallOutcome {
    let mut request = Request::new(futures::stream::iter(vec![()]));
    let metadata = request.metadata_mut();
    match credential {
        Credential::Bearer(token) => {
            metadata.insert(
                "authorization",
                format!("Bearer {token}")
                    .parse()
                    .expect("bearer authorization metadata"),
            );
        }
        Credential::ApiKey(key) => {
            metadata.insert("x-api-key", key.parse().expect("API key metadata"));
        }
    }
    metadata.insert("x-tenant-id", tenant_id.parse().expect("tenant metadata"));
    metadata.insert(
        "x-udb-project-id",
        project_id.parse().expect("project metadata"),
    );
    if let Some(request_id) = request_id {
        metadata.insert("x-request-id", request_id.parse().expect("request id"));
    }
    let path_and_query: PathAndQuery = path.parse().expect("native RPC path parses");
    let mut grpc = tonic::client::Grpc::new(channel);
    let call = async move {
        if let Err(err) = grpc.ready().await {
            return CallOutcome::Transport(format!("test channel not ready: {err}"));
        }
        let codec = tonic::codec::ProstCodec::<(), ()>::default();
        match grpc.streaming(request, path_and_query, codec).await {
            Err(status) => CallOutcome::Failed(status),
            Ok(response) => {
                let mut stream = response.into_inner();
                match tokio::time::timeout(FIRST_MESSAGE_WAIT, stream.message()).await {
                    Ok(Ok(_)) => CallOutcome::Answered,
                    Ok(Err(status)) => CallOutcome::Failed(status),
                    Err(_) => CallOutcome::Pending,
                }
            }
        }
    };
    tokio::time::timeout(CALL_TIMEOUT, call)
        .await
        .unwrap_or(CallOutcome::Pending)
}

fn error_detail(status: &tonic::Status) -> Option<crate::proto::ErrorDetail> {
    use prost::Message as _;
    let raw = status
        .metadata()
        .get_bin(crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY)?
        .to_bytes()
        .ok()?;
    crate::proto::ErrorDetail::decode(raw.as_ref()).ok()
}

/// The native service behind `path` is not mounted on this node, so the call
/// never reached the security gates.
fn is_disabled_service(status: &tonic::Status) -> bool {
    status.code() == tonic::Code::FailedPrecondition
        && error_detail(status)
            .is_some_and(|detail| detail.capability_required == "native_service_enabled")
}

/// `Some(reason)` when `status` is a denial by an authentication/authorization
/// gate rather than a business answer from the handler.
fn gate_denial(status: &tonic::Status) -> Option<&'static str> {
    let detail = error_detail(status);
    if detail
        .as_ref()
        .is_some_and(|detail| detail.operation == "method_security")
    {
        return Some("method_security deny");
    }
    let message = status.message().to_ascii_lowercase();
    match status.code() {
        // Every gate-raised Unauthenticated is a plain status without a typed
        // detail; a handler's own authentication answer carries one.
        tonic::Code::Unauthenticated if detail.is_none() => Some("unauthenticated"),
        tonic::Code::PermissionDenied if message.contains("scope") => Some("handler scope deny"),
        tonic::Code::PermissionDenied if message.contains("credential") => {
            Some("handler credential deny")
        }
        tonic::Code::InvalidArgument | tonic::Code::PermissionDenied
            if message.contains("request context") =>
        {
            Some("request context deny")
        }
        _ => None,
    }
}

async fn create_service_api_key(
    apikey: &crate::runtime::service::auth_service::ApiKeyServiceImpl,
    owner: &authn_entity_pb::User,
    name: &str,
    scopes: &[String],
) -> String {
    // The key must live in the owner's CANONICAL tenant/project (the user and
    // its grant carry the resolved tenant UUID, not the `acme` alias): a key
    // minted under the alias exchanges into a bearer the grant check rejects.
    let owner_id = owner.user_id.as_str();
    let (tenant_id, project_id) = (owner.tenant_id.as_str(), owner.project_id.as_str());
    scope_claim_context_for_test(
        test_claim_context(owner_id, tenant_id, project_id, &[], &[]),
        apikey.create_api_key(Request::new(apikey_pb::CreateApiKeyRequest {
            name: name.to_string(),
            owner_id: owner_id.to_string(),
            scopes: scopes.to_vec(),
            context: Some(common_pb::RequestContext {
                principal_id: owner_id.to_string(),
                tenant: Some(common_pb::TenantContext {
                    tenant_id: tenant_id.to_string(),
                    project_id: project_id.to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        })),
    )
    .await
    .unwrap_or_else(|err| panic!("create service API key {name}: {err}"))
    .into_inner()
    .plain_key
}

async fn exchange_api_key(
    authn: &crate::runtime::service::auth_service::AuthnServiceImpl,
    plain_key: &str,
) -> authn_pb::AuthnResponse {
    authn
        .authenticate(Request::new(authn_pb::AuthnRequest {
            api_key: plain_key.to_string(),
            ..Default::default()
        }))
        .await
        .expect("exchange service API key for a bearer")
        .into_inner()
}

#[tokio::test]
#[ignore = "requires live Postgres; run in the CI live lane"]
async fn live_service_account_reaches_every_service_callable_native_method() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;

    let callable = service_callable_paths();
    assert!(
        callable.len() >= SERVICE_CALLABLE_PATH_FLOOR,
        "service-callable native RPCs dropped to {} (floor {SERVICE_CALLABLE_PATH_FLOOR}); a \
         method stopped admitting CREDENTIAL_TYPE_SERVICE_ACCOUNT/API_KEY",
        callable.len()
    );

    // The union of every service-callable method's declared scopes that a grant
    // may legally carry. A method whose only scopes are forbidden for services
    // stays out of the grant and surfaces below as a scope denial.
    let grant_scopes: Vec<String> = callable
        .iter()
        .flat_map(|(_, security)| security.scopes.iter())
        .map(|scope| scope.trim().to_string())
        .filter(|scope| {
            !scope.is_empty()
                && crate::runtime::service::auth_service::grants::validate_service_scopes(
                    &[],
                    std::slice::from_ref(scope),
                )
                .is_ok()
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    assert!(
        !grant_scopes.is_empty(),
        "service-callable methods must declare grantable scopes"
    );

    let security = crate::runtime::security::SecurityConfig {
        jwt_private_key: Some(include_str!("../../../testdata/jwt_rs256_private.pem").to_string()),
        jwt_public_key: Some(include_str!("../../../testdata/jwt_rs256_public.pem").to_string()),
        ..crate::runtime::security::SecurityConfig::default()
    };
    crate::runtime::security::SecurityConfig::install_global(security.clone());
    let authn = authn_service_with_jwt(pool.clone());
    let apikey = api_key_service(pool.clone());
    let grant_scope_refs: Vec<&str> = grant_scopes.iter().map(String::as_str).collect();
    let (owner, _grant) = create_service_account_with_grant(
        &authn,
        "svc_conformance",
        "CorrectHorse1!",
        &grant_scope_refs,
    )
    .await;
    let plain_key =
        create_service_api_key(&apikey, &owner, "svc-conformance-key", &grant_scopes).await;
    let exchanged = exchange_api_key(&authn, &plain_key).await;
    assert!(
        !exchanged.access_token.is_empty(),
        "API-key exchange must mint a bearer"
    );
    let principal = exchanged
        .principal
        .clone()
        .expect("exchanged API-key principal");
    let tenant_id = principal.tenant_id.clone();
    let project_id = if principal.project_id.is_empty() {
        owner.project_id.clone()
    } else {
        principal.project_id.clone()
    };
    assert!(
        !tenant_id.is_empty(),
        "service principal must carry a tenant"
    );

    // Precheck the two durable checks the credential resolver runs, with the
    // inputs in the message: the served gate reports only "invalid bearer
    // token", so a failure here names the actual cause.
    let claims = crate::runtime::security::validate_bearer_token(
        &crate::runtime::security::SecurityConfig::current(),
        &exchanged.access_token,
    )
    .expect("the exchanged bearer verifies against the installed keys");
    let grant_check = super::super::grants::validate_service_principal_against_grant(
        &pool,
        claims.tenant_id.as_deref().unwrap_or_default(),
        claims.sub.as_deref().unwrap_or_default(),
        claims.project_id.as_deref().unwrap_or_default(),
        claims.service_identity.as_deref().unwrap_or_default(),
        &claims.resolved_scopes(),
    )
    .await;
    assert_eq!(
        grant_check,
        Ok(true),
        "grant check for tenant={:?} sub={:?} project={:?} identity={:?} ({} scopes)",
        claims.tenant_id,
        claims.sub,
        claims.project_id,
        claims.service_identity,
        claims.resolved_scopes().len()
    );
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after epoch")
        .as_secs();
    let durable = authn.jwt_persisted_state_valid(&claims, now).await;
    assert!(
        matches!(durable, Ok(true)),
        "durable bearer state for the exchanged key: {durable:?}"
    );

    // Serve the native plane exactly as production mounts it.
    let broker = native_broker_service().await;
    activate_live_project_catalog(&broker, &project_id, "service-caller-conformance").await;
    let routes = crate::runtime::service::NativeControlPlaneServices::from_broker(&broker)
        .into_routes(&MethodSecurityLayer::new());
    // Installed AFTER the services are built: building the runtime installs the
    // process-global SecurityConfig from env, and `build_auth_services`
    // re-installs the credential resolvers with the broker's own config; either
    // would silently replace the test's (the served bearer then fails to verify).
    crate::runtime::security::SecurityConfig::install_global(security.clone());
    crate::runtime::service::auth_service::install_data_plane_credential_resolvers(
        pool.clone(),
        &crate::runtime::authn::AuthnConfig {
            session_enabled: true,
            session_hash_secret: "live-auth-test-secret".to_string(),
            ..crate::runtime::authn::AuthnConfig::default()
        },
        Arc::new(authn.clone()),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind native conformance listener");
    let address = listener
        .local_addr()
        .expect("native conformance listener address");
    let incoming = futures::stream::unfold(listener, |listener| async move {
        let connection = listener.accept().await.map(|(stream, _)| stream);
        Some((connection, listener))
    });
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .layer(crate::runtime::credential_layer::CredentialResolveLayer::new())
            .add_routes(routes)
            .serve_with_incoming_shutdown(incoming, async move {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("serve native conformance listener");
    });
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
        .expect("native conformance endpoint")
        .connect()
        .await
        .expect("connect native conformance channel");

    // Positive half: every service-callable method, with each credential form.
    let bearer = exchanged.access_token.clone();
    let mut calls = Vec::new();
    for (path, _) in &callable {
        for credential in [Credential::Bearer(&bearer), Credential::ApiKey(&plain_key)] {
            calls.push((path.clone(), credential));
        }
    }
    let results: Vec<(String, &'static str, CallOutcome)> = futures::stream::iter(calls)
        .enumerate()
        .map(|(index, (path, credential))| {
            let channel = channel.clone();
            let tenant_id = tenant_id.clone();
            let project_id = project_id.clone();
            async move {
                let request_id = format!("svc-conformance-{index}");
                let outcome = call_native(
                    channel,
                    &path,
                    credential,
                    &tenant_id,
                    &project_id,
                    Some(&request_id),
                )
                .await;
                (path, credential.label(), outcome)
            }
        })
        .buffer_unordered(CONCURRENT_CALLS)
        .collect()
        .await;

    let mut failures = Vec::new();
    let mut disabled = BTreeSet::new();
    let mut reached = 0usize;
    for (path, label, outcome) in &results {
        match outcome {
            CallOutcome::Answered | CallOutcome::Pending => reached += 1,
            CallOutcome::Failed(status) if is_disabled_service(status) => {
                disabled.insert(path.clone());
            }
            CallOutcome::Failed(status) => match gate_denial(status) {
                Some(reason) => failures.push(format!(
                    "{path} [{label}] → {:?} ({reason}): {}",
                    status.code(),
                    status.message()
                )),
                None => reached += 1,
            },
            CallOutcome::Transport(error) => {
                failures.push(format!("{path} [{label}] → transport: {error}"))
            }
        }
    }
    failures.sort();
    if !disabled.is_empty() {
        eprintln!(
            "service-caller conformance: {} path(s) belong to native services not mounted on \
             this node and never reached the gates:\n  {}",
            disabled.len(),
            disabled.iter().cloned().collect::<Vec<_>>().join("\n  ")
        );
    }
    assert!(
        failures.is_empty(),
        "{} of {} service-account calls were denied by a security gate:\n{}",
        failures.len(),
        results.len(),
        failures.join("\n")
    );
    assert!(
        reached > 0,
        "no service-account call reached a handler; every native service is disabled here, so \
         the gates were never exercised"
    );

    // Negative half 1: without any request-context header, a method that
    // requires one is refused and the refusal names the request context.
    let (context_path, _) = callable
        .iter()
        .find(|(_, security)| security.request_context_required)
        .expect("at least one service-callable method requires request context");
    match call_native(
        channel.clone(),
        context_path,
        Credential::Bearer(&bearer),
        &tenant_id,
        &project_id,
        None,
    )
    .await
    {
        CallOutcome::Failed(status) => {
            assert!(
                matches!(
                    status.code(),
                    tonic::Code::InvalidArgument | tonic::Code::PermissionDenied
                ),
                "{context_path} without request context: unexpected code {:?}: {}",
                status.code(),
                status.message()
            );
            assert!(
                status
                    .message()
                    .to_ascii_lowercase()
                    .contains("request context"),
                "{context_path} without request context must name it: {}",
                status.message()
            );
        }
        _ => panic!("{context_path} without request context must be refused"),
    }

    // Negative half 2: a key whose grant-attenuated scopes lack one method's
    // scopes is refused on that method, and the deny names the method's scope.
    let (scope_path, scope_security) = callable
        .iter()
        .find(|(_, security)| {
            !security.scopes.is_empty()
                && security
                    .scopes
                    .iter()
                    .all(|scope| grant_scopes.iter().any(|g| g.eq_ignore_ascii_case(scope)))
        })
        .expect("a service-callable method with grantable scopes");
    let reduced_scopes: Vec<String> = grant_scopes
        .iter()
        .filter(|granted| {
            !scope_security
                .scopes
                .iter()
                .any(|scope| scope.eq_ignore_ascii_case(granted))
        })
        .cloned()
        .collect();
    assert!(
        !reduced_scopes.is_empty(),
        "removing {scope_path}'s scopes must leave a non-empty key"
    );
    let reduced_key = create_service_api_key(
        &apikey,
        &owner,
        "svc-conformance-reduced-key",
        &reduced_scopes,
    )
    .await;
    let reduced = exchange_api_key(&authn, &reduced_key).await;
    match call_native(
        channel.clone(),
        scope_path,
        Credential::Bearer(&reduced.access_token),
        &tenant_id,
        &project_id,
        Some("svc-conformance-scope-negative"),
    )
    .await
    {
        CallOutcome::Failed(status) => {
            assert_eq!(
                status.code(),
                tonic::Code::PermissionDenied,
                "{scope_path} without its scope: {}",
                status.message()
            );
            assert!(
                scope_security
                    .scopes
                    .iter()
                    .any(|scope| status.message().contains(scope.as_str())),
                "{scope_path} scope deny must name the method scope {:?}: {}",
                scope_security.scopes,
                status.message()
            );
        }
        _ => panic!("{scope_path} must be refused without its scope"),
    }

    let _ = shutdown_tx.send(());
    server.await.expect("native conformance server task");
    cleanup_native_auth_db(&pool).await;
}
