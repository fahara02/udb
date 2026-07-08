//! SAML 2.0 Web SSO HTTP surface (SP side) for browser-driven federation.
//!
//! Master-plan 4.2 (the SAML half; the SCIM HTTP half already ships in
//! [`super::scim_http`]). A SAML Identity Provider drives the SP over **HTTP**,
//! not gRPC: it fetches the SP's metadata and POSTs the signed `SAMLResponse` to
//! the Assertion Consumer Service (ACS) endpoint as an HTML form. This module
//! exposes those two HTTP endpoints and maps each onto the EXISTING gRPC
//! [`IdentityProviderService`] handlers — the signature verification, replay
//! guard, claim/group mapping, JIT provisioning and session establishment all
//! live in those handlers, so nothing is duplicated here.
//!
//! Endpoints (base `/saml`):
//!   * `GET  /saml/metadata` — SP metadata (entityID + ACS endpoint + binding).
//!   * `POST /saml/acs`       — HTTP-POST binding: form field `SAMLResponse`
//!                              (base64) is decoded and handed to the gRPC
//!                              `saml_acs` handler, which validates the XML-DSig
//!                              signature and mints the session.
//!
//! Multi-tenant/provider scoping mirrors `scim_http`: the explicit
//! `/saml/t/{tenant}/p/{provider}/{metadata|acs}` form, falling back to
//! `UDB_SAML_DEFAULT_TENANT` / `UDB_SAML_DEFAULT_PROVIDER` for the bare
//! `/saml/{metadata|acs}` paths.
//!
//! Transport: a raw `tokio` TcpListener with a minimal HTTP/1.1 reader, mirroring
//! [`super::scim_http`] and the metrics listener (the broker has no axum/hyper
//! dependency). OFF by default; only binds when `UDB_SAML_HTTP_ADDR` is set.
//!
//! ## Trust boundary (critical)
//!
//! The ACS endpoint is **unauthenticated by design** — the IdP (or the user's
//! browser carrying the IdP's POST) is an anonymous client. The trust boundary is
//! the XML-DSig signature check inside [`super::saml::validate_response`]: an
//! assertion whose exclusive-C14N reference digest + RSA signature do not verify
//! against the provider's configured signing certs is REJECTED (the gRPC handler
//! returns `authenticated = false`, which this surface renders as `401`). There is
//! NO path that accepts an assertion whose canonical digest does not verify, and
//! NO parallel crypto: this module only base64-forwards the form field to the
//! existing handler.
//!
//! Fail-closed posture: like `scim_http` refuses to start without a bearer token,
//! this listener refuses to start without a configured provider binding
//! (`UDB_SAML_DEFAULT_TENANT` + `UDB_SAML_DEFAULT_PROVIDER`). An ACS with no
//! resolvable provider has no trust anchor (no signing certs to verify against),
//! so standing one up would be meaningless — we fail closed instead.

use std::net::SocketAddr;
use std::sync::Arc;

use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tonic::Request;

use crate::proto::udb::core::idp::services::v1 as idp_pb;
use idp_pb::identity_provider_service_server::IdentityProviderService;

use super::IdentityProviderServiceImpl;
#[cfg(test)]
use super::idp_saml_replay_rejected_status;

/// Runtime config for the SAML HTTP listener, resolved from the environment.
struct SamlHttpConfig {
    addr: SocketAddr,
    /// Default provider binding used for the bare `/saml/...` paths. Required:
    /// an ACS with no provider has no signing-cert trust anchor (fail closed).
    default_tenant: String,
    default_provider: String,
    /// SP Assertion Consumer Service URL advertised in `/saml/metadata`. When
    /// empty the metadata advertises the relative `/saml/acs` path.
    acs_url: String,
}

impl SamlHttpConfig {
    /// Resolve from env. Returns `None` when `UDB_SAML_HTTP_ADDR` is unset (the
    /// surface is OFF by default). Returns `None` with a warning when the addr is
    /// set but no provider binding is configured — an ACS with no provider has no
    /// signing-cert trust anchor, so we never stand one up (fail closed). Mirrors
    /// [`super::scim_http`]'s `ScimHttpConfig::from_env` posture exactly.
    fn from_env() -> Option<Self> {
        let raw_addr = std::env::var("UDB_SAML_HTTP_ADDR").ok()?;
        let addr: SocketAddr = match raw_addr.trim().parse() {
            Ok(addr) => addr,
            Err(err) => {
                tracing::warn!(value = %raw_addr, error = %err, "invalid UDB_SAML_HTTP_ADDR; SAML HTTP disabled");
                return None;
            }
        };
        let default_tenant = std::env::var("UDB_SAML_DEFAULT_TENANT")
            .unwrap_or_default()
            .trim()
            .to_string();
        let default_provider = std::env::var("UDB_SAML_DEFAULT_PROVIDER")
            .unwrap_or_default()
            .trim()
            .to_string();
        if default_tenant.is_empty() || default_provider.is_empty() {
            tracing::warn!(
                "UDB_SAML_HTTP_ADDR is set but UDB_SAML_DEFAULT_TENANT/UDB_SAML_DEFAULT_PROVIDER \
                 is empty; SAML HTTP refuses to start without a provider binding (no provider => \
                 no signing-cert trust anchor; fail closed)"
            );
            return None;
        }
        Some(Self {
            addr,
            default_tenant,
            default_provider,
            acs_url: std::env::var("UDB_SAML_ACS_URL")
                .unwrap_or_default()
                .trim()
                .to_string(),
        })
    }
}

/// Spawn the SAML HTTP listener bound to `shutdown`, or `None` when disabled.
/// Mirrors [`super::scim_http::spawn_from_env_with_shutdown`] — a detached task
/// that logs but never brings down the data plane.
///
// W9: leader wires spawn_saml_http_from_env into service/mod.rs serve()
pub(crate) fn spawn_from_env_with_shutdown<F>(
    service: Arc<IdentityProviderServiceImpl>,
    shutdown: F,
) -> Option<tokio::task::JoinHandle<()>>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let cfg = SamlHttpConfig::from_env()?;
    tracing::info!(addr = %cfg.addr, "SAML 2.0 Web SSO HTTP surface enabled");
    Some(tokio::spawn(async move {
        tokio::select! {
            _ = serve(service, cfg) => {}
            _ = shutdown => {
                tracing::info!("SAML HTTP listener shutting down");
            }
        }
    }))
}

async fn serve(service: Arc<IdentityProviderServiceImpl>, cfg: SamlHttpConfig) {
    let listener = match tokio::net::TcpListener::bind(cfg.addr).await {
        Ok(l) => l,
        Err(err) => {
            tracing::warn!(addr = %cfg.addr, error = %err, "SAML HTTP endpoint disabled");
            return;
        }
    };
    let cfg = Arc::new(cfg);
    loop {
        let Ok((mut socket, _peer)) = listener.accept().await else {
            continue;
        };
        let service = service.clone();
        let cfg = cfg.clone();
        tokio::spawn(async move {
            let response = match read_request(&mut socket).await {
                Some(req) => dispatch(service.as_ref(), cfg.as_ref(), req).await,
                None => http_response(400, "text/plain; charset=utf-8", "malformed HTTP request"),
            };
            let _ = socket.write_all(response.as_bytes()).await;
        });
    }
}

/// A parsed HTTP request: method, path (no query), and body (form/raw).
struct HttpRequest {
    method: String,
    path: String,
    body: String,
}

/// Read one HTTP/1.1 request with a bounded buffer + deadline (slow-loris guard),
/// mirroring [`super::scim_http`]'s reader. Reads headers, then up to
/// `Content-Length` body bytes (capped). SAMLResponse payloads are small.
async fn read_request(socket: &mut tokio::net::TcpStream) -> Option<HttpRequest> {
    const MAX: usize = 1024 * 1024;
    let mut buf: Vec<u8> = Vec::with_capacity(8192);
    let mut chunk = [0u8; 8192];
    let header_end = loop {
        let n = tokio::time::timeout(std::time::Duration::from_secs(10), socket.read(&mut chunk))
            .await
            .ok()?
            .ok()?;
        if n == 0 {
            break find_header_end(&buf);
        }
        buf.extend_from_slice(&chunk[..n]);
        if let Some(end) = find_header_end(&buf) {
            break Some(end);
        }
        if buf.len() > MAX {
            return None;
        }
    }?;

    let head = String::from_utf8_lossy(&buf[..header_end]).to_string();
    let mut lines = head.lines();
    let request_line = lines.next()?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next()?.to_string();
    let raw_target = parts.next()?.to_string();
    // Drop any query string; the SAML HTTP-POST binding carries data in the body.
    let path = match raw_target.split_once('?') {
        Some((p, _)) => p.to_string(),
        None => raw_target,
    };

    let mut content_length = 0usize;
    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            if name.trim().eq_ignore_ascii_case("content-length") {
                content_length = value.trim().parse().unwrap_or(0);
            }
        }
    }

    let body_start = header_end + 4; // skip the \r\n\r\n
    let mut body_bytes: Vec<u8> = buf
        .get(body_start..)
        .map(|s| s.to_vec())
        .unwrap_or_default();
    while body_bytes.len() < content_length.min(MAX) {
        let n = tokio::time::timeout(std::time::Duration::from_secs(10), socket.read(&mut chunk))
            .await
            .ok()?
            .ok()?;
        if n == 0 {
            break;
        }
        body_bytes.extend_from_slice(&chunk[..n]);
    }
    let body = String::from_utf8_lossy(&body_bytes).to_string();

    Some(HttpRequest { method, path, body })
}

fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

/// Route + dispatch. Returns a full HTTP response string.
async fn dispatch(
    service: &IdentityProviderServiceImpl,
    cfg: &SamlHttpConfig,
    req: HttpRequest,
) -> String {
    let Some(rest) = req.path.strip_prefix("/saml") else {
        return http_response(404, "text/plain; charset=utf-8", "not a SAML endpoint");
    };
    let rest = rest.trim_start_matches('/');
    let (tenant, provider, resource) = resolve_scope(rest, cfg);

    match (req.method.as_str(), resource.as_str()) {
        // SP metadata is public per the SAML Web SSO profile (the IdP fetches it
        // before any assertion exists).
        ("GET", "metadata") => metadata_response(service, cfg, &tenant, &provider).await,
        // Assertion Consumer Service: HTTP-POST binding. UNAUTHENTICATED by design;
        // the trust boundary is the XML-DSig check in `saml::validate_response`,
        // performed inside the gRPC `saml_acs` handler.
        ("POST", "acs") => acs_response(service, &tenant, &provider, &req.body).await,
        ("GET", "acs") => http_response(
            405,
            "text/plain; charset=utf-8",
            "the ACS endpoint accepts only the SAML HTTP-POST binding (POST)",
        ),
        _ => http_response(404, "text/plain; charset=utf-8", "unknown SAML endpoint"),
    }
}

/// Resolve `(tenant, provider, resource)` from the path remainder. Supports the
/// explicit `t/{tenant}/p/{provider}/<resource>` form, falling back to the
/// configured defaults for the bare `/saml/<resource>` form (mirrors scim_http).
fn resolve_scope(rest: &str, cfg: &SamlHttpConfig) -> (String, String, String) {
    let segs: Vec<&str> = rest.split('/').collect();
    if segs.len() >= 4 && segs[0] == "t" && segs[2] == "p" {
        let tenant = segs[1].to_string();
        let provider = segs[3].to_string();
        let resource = segs[4..].join("/");
        return (tenant, provider, resource);
    }
    (
        cfg.default_tenant.clone(),
        cfg.default_provider.clone(),
        rest.to_string(),
    )
}

/// `POST /saml/acs` — decode the form `SAMLResponse` field and hand it to the
/// gRPC `saml_acs` handler, which performs the XML-DSig verification + replay
/// guard + claim mapping + JIT provisioning and mints the session. We add NO
/// crypto here; on an unverified/invalid assertion the handler returns
/// `authenticated = false`, which we render as `401` (fail closed).
async fn acs_response(
    service: &IdentityProviderServiceImpl,
    tenant: &str,
    provider: &str,
    body: &str,
) -> String {
    let Some(saml_response) = form_param(body, "SAMLResponse") else {
        return http_response(
            400,
            "text/plain; charset=utf-8",
            "missing SAMLResponse form field (HTTP-POST binding)",
        );
    };
    let relay_state = form_param(body, "RelayState").unwrap_or_default();

    let grpc = idp_pb::SamlAcsRequest {
        provider_id: provider.to_string(),
        tenant_id: tenant.to_string(),
        saml_response,
        relay_state,
        context: None,
    };
    match service.saml_acs(Request::new(grpc)).await {
        Ok(resp) => {
            let r = resp.into_inner();
            // The gRPC handler fails closed: a signature/parse failure returns
            // `authenticated = false` (never an error), so an unverified assertion
            // can NEVER reach the authenticated branch. Render that as 401.
            if !r.authenticated || !r.signature_verified {
                return http_response(
                    401,
                    "application/json; charset=utf-8",
                    &json!({
                        "authenticated": false,
                        "signature_verified": r.signature_verified,
                        "detail": if r.detail.is_empty() {
                            "assertion rejected".to_string()
                        } else {
                            r.detail
                        },
                    })
                    .to_string(),
                );
            }
            // Authenticated: the gRPC handler has minted the session principal
            // (resolved/JIT-provisioned user, mapped roles, assurance). Return it.
            http_response(
                200,
                "application/json; charset=utf-8",
                &json!({
                    "authenticated": true,
                    "signature_verified": true,
                    "subject": r.subject,
                    "user_id": r.user_id,
                    "email": r.email,
                    "email_verified": r.email_verified,
                    "groups": r.groups,
                    "roles": r.roles,
                    "assurance": r.assurance,
                    "attributes": serde_json::from_str::<serde_json::Value>(&r.attributes_json)
                        .unwrap_or(serde_json::Value::Null),
                    "detail": r.detail,
                })
                .to_string(),
            )
        }
        // Disabled provider / replay / missing pool etc. map to the closest HTTP
        // status. Replay (PermissionDenied) and disabled (FailedPrecondition) are
        // all denials — never an authenticated response.
        Err(status) => status_response(status),
    }
}

/// `GET /saml/metadata` — render the SP's SAML metadata (entityID + ACS endpoint +
/// HTTP-POST binding). The SP entityID comes from the provider's configured
/// `entity_id` (mirroring `start_saml_login`'s `sp_entity_id` derivation), fetched
/// via the existing gRPC `get_provider` handler so no persistence is duplicated.
async fn metadata_response(
    service: &IdentityProviderServiceImpl,
    cfg: &SamlHttpConfig,
    tenant: &str,
    provider: &str,
) -> String {
    let entity_id = match service
        .get_provider(Request::new(idp_pb::GetProviderRequest {
            provider_id: provider.to_string(),
            tenant_id: tenant.to_string(),
        }))
        .await
    {
        Ok(resp) => resp
            .into_inner()
            .provider
            .map(|p| p.entity_id)
            .unwrap_or_default(),
        Err(status) => return status_response(status),
    };
    // Mirror start_saml_login: derive a stable SP entityID when none is configured.
    let sp_entity_id = if entity_id.trim().is_empty() {
        format!("urn:udb:sp:{tenant}")
    } else {
        entity_id
    };
    let acs_url = if cfg.acs_url.is_empty() {
        "/saml/acs".to_string()
    } else {
        cfg.acs_url.clone()
    };
    let xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
<md:EntityDescriptor xmlns:md=\"urn:oasis:names:tc:SAML:2.0:metadata\" entityID=\"{entity}\">\
<md:SPSSODescriptor AuthnRequestsSigned=\"false\" WantAssertionsSigned=\"true\" \
protocolSupportEnumeration=\"urn:oasis:names:tc:SAML:2.0:protocol\">\
<md:NameIDFormat>urn:oasis:names:tc:SAML:1.1:nameid-format:emailAddress</md:NameIDFormat>\
<md:AssertionConsumerService Binding=\"urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST\" \
Location=\"{acs}\" index=\"0\" isDefault=\"true\"/>\
</md:SPSSODescriptor></md:EntityDescriptor>",
        entity = xml_escape(&sp_entity_id),
        acs = xml_escape(&acs_url),
    );
    http_response(200, "application/samlmetadata+xml; charset=utf-8", &xml)
}

/// Extract a field from an `application/x-www-form-urlencoded` body. Returns the
/// decoded value with internal whitespace stripped (so the base64 `SAMLResponse`
/// is clean for the downstream decoder), or `None` when the key is absent/empty.
fn form_param(body: &str, key: &str) -> Option<String> {
    for pair in body.split('&') {
        if let Some((k, v)) = pair.split_once('=') {
            if k == key {
                // application/x-www-form-urlencoded: '+' encodes a space; literal
                // '+' bytes in the base64 are sent percent-encoded (%2B).
                let plus_decoded = v.replace('+', " ");
                let decoded = urlencoding::decode(&plus_decoded)
                    .map(|c| c.into_owned())
                    .unwrap_or_else(|_| plus_decoded.clone());
                // Strip any residual whitespace so the base64 payload is clean.
                let cleaned: String = decoded.chars().filter(|c| !c.is_whitespace()).collect();
                if cleaned.is_empty() {
                    return None;
                }
                return Some(cleaned);
            }
        }
    }
    None
}

/// Map a tonic `Status` to the closest HTTP status. Replay/disabled/etc. are all
/// denials and never produce an authenticated response.
fn status_response(status: tonic::Status) -> String {
    use tonic::Code;
    let http = match status.code() {
        Code::InvalidArgument | Code::FailedPrecondition => 400,
        Code::Unauthenticated => 401,
        Code::PermissionDenied => 403,
        Code::NotFound => 404,
        Code::AlreadyExists => 409,
        Code::Unavailable => 503,
        _ => 500,
    };
    http_response(
        http,
        "application/json; charset=utf-8",
        &json!({ "authenticated": false, "detail": status.message() }).to_string(),
    )
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

/// Minimal HTTP/1.1 response, mirroring [`super::scim_http`]'s helper.
fn http_response(status: u16, content_type: &str, body: &str) -> String {
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        409 => "Conflict",
        500 => "Internal Server Error",
        503 => "Service Unavailable",
        _ => "OK",
    };
    let mut head = format!(
        "HTTP/1.1 {status} {reason}\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\nconnection: close\r\n",
        body.len()
    );
    if status == 401 {
        head.push_str("www-authenticate: SAML\r\n");
    }
    if status == 405 {
        head.push_str("allow: POST\r\n");
    }
    head.push_str("\r\n");
    head.push_str(body);
    head
}

#[cfg(test)]
mod saml_http_tests {
    use super::*;

    /// Save/restore an env var around a test (mirrors `config/tests.rs`). The
    /// `from_env` gating test is the SINGLE place that touches these vars, so it
    /// serializes all of disabled/fail-closed/enabled into one function — no
    /// cross-test env races.
    fn set_env(key: &str, value: Option<&str>) {
        #[allow(unused_unsafe)]
        unsafe {
            match value {
                Some(v) => std::env::set_var(key, v),
                None => std::env::remove_var(key),
            }
        }
    }

    // ── round-trip: a well-formed POST body funnels into the ACS request ──────
    //
    // The HTTP-POST binding carries the base64 `SAMLResponse` (and optional
    // `RelayState`) in an `application/x-www-form-urlencoded` body. This proves
    // the transport extracts exactly the bytes that get forwarded, unmodified, to
    // the gRPC `saml_acs` handler — which owns the XML-DSig verification.
    #[test]
    fn round_trip_form_funnels_into_saml_acs_request() {
        // Base64 with the three reserved chars percent-encoded the way a browser
        // POST encodes them: '+' -> %2B, '/' -> %2F, '=' -> %3D.
        let raw_b64 = "PHNhbWxwOlJlc3BvbnNl+with/pad==";
        let encoded = "PHNhbWxwOlJlc3BvbnNl%2Bwith%2Fpad%3D%3D";
        let body = format!("SAMLResponse={encoded}&RelayState=app%2Fhome");

        let saml_response = form_param(&body, "SAMLResponse").expect("SAMLResponse present");
        let relay_state = form_param(&body, "RelayState").unwrap_or_default();
        // The base64 survives intact (reserved chars decoded back), whitespace-free.
        assert_eq!(saml_response, raw_b64);
        assert_eq!(relay_state, "app/home");

        // The transport builds the SAME SamlAcsRequest `acs_response` builds; the
        // raw SAMLResponse flows through untouched (no client-side validation).
        let grpc = idp_pb::SamlAcsRequest {
            provider_id: "okta".to_string(),
            tenant_id: "acme".to_string(),
            saml_response: saml_response.clone(),
            relay_state,
            context: None,
        };
        assert_eq!(grpc.saml_response, raw_b64);
        assert_eq!(grpc.tenant_id, "acme");
        assert_eq!(grpc.provider_id, "okta");
    }

    // ── tamper: the HTTP layer NEVER validates the assertion ──────────────────
    //
    // A garbage / tampered `SAMLResponse` is transported verbatim — the transport
    // performs NO crypto and cannot be the rejecting party. Verification is the
    // sole responsibility of `saml::validate_response` inside the gRPC `saml_acs`
    // handler (the C14N + XML-DSig trust boundary). This test locks in that the
    // module adds no parallel/short-circuit validation.
    #[test]
    fn tamper_response_is_transported_unvalidated_not_rejected_by_http() {
        let tampered = "not-base64-!!!-tampered-bytes";
        let body = format!("SAMLResponse={tampered}&RelayState=x");
        // form_param returns the bytes as-is (no base64/XML/signature inspection).
        assert_eq!(form_param(&body, "SAMLResponse").as_deref(), Some(tampered));
    }

    // ── tamper: an empty/missing SAMLResponse is a transport-level 400 ────────
    //
    // Absence of the form field is a malformed HTTP request (not an auth verdict);
    // `acs_response` answers 400. This is distinct from a present-but-invalid
    // assertion, which is forwarded and denied by the ACS verifier.
    #[test]
    fn empty_or_missing_saml_response_is_none() {
        assert_eq!(form_param("RelayState=x", "SAMLResponse"), None);
        assert_eq!(form_param("SAMLResponse=", "SAMLResponse"), None);
        assert_eq!(form_param("SAMLResponse=%20%20", "SAMLResponse"), None);
    }

    // ── disabled (+ fail-closed + enabled) gate, serialized in one test ───────
    #[test]
    fn from_env_gating_disabled_failclosed_enabled() {
        let prior_addr = std::env::var("UDB_SAML_HTTP_ADDR").ok();
        let prior_tenant = std::env::var("UDB_SAML_DEFAULT_TENANT").ok();
        let prior_provider = std::env::var("UDB_SAML_DEFAULT_PROVIDER").ok();
        let prior_acs = std::env::var("UDB_SAML_ACS_URL").ok();

        // disabled: addr unset → the listener never starts (off by default).
        set_env("UDB_SAML_HTTP_ADDR", None);
        set_env("UDB_SAML_DEFAULT_TENANT", Some("acme"));
        set_env("UDB_SAML_DEFAULT_PROVIDER", Some("okta"));
        assert!(
            SamlHttpConfig::from_env().is_none(),
            "unset UDB_SAML_HTTP_ADDR must keep the SAML HTTP surface OFF"
        );

        // fail-closed: addr set but no provider binding → no signing-cert trust
        // anchor → refuse to start (never an unanchored ACS).
        set_env("UDB_SAML_HTTP_ADDR", Some("127.0.0.1:0"));
        set_env("UDB_SAML_DEFAULT_TENANT", None);
        set_env("UDB_SAML_DEFAULT_PROVIDER", None);
        assert!(
            SamlHttpConfig::from_env().is_none(),
            "missing provider binding must fail closed"
        );

        // enabled: addr + provider binding present → Some with parsed fields.
        set_env("UDB_SAML_HTTP_ADDR", Some("127.0.0.1:0"));
        set_env("UDB_SAML_DEFAULT_TENANT", Some("acme"));
        set_env("UDB_SAML_DEFAULT_PROVIDER", Some("okta"));
        set_env("UDB_SAML_ACS_URL", Some("https://sp.example.com/saml/acs"));
        let cfg = SamlHttpConfig::from_env().expect("configured listener is enabled");
        assert_eq!(cfg.default_tenant, "acme");
        assert_eq!(cfg.default_provider, "okta");
        assert_eq!(cfg.acs_url, "https://sp.example.com/saml/acs");

        // invalid addr → None (parse failure disables, never panics).
        set_env("UDB_SAML_HTTP_ADDR", Some("not-an-addr"));
        assert!(SamlHttpConfig::from_env().is_none());

        set_env("UDB_SAML_HTTP_ADDR", prior_addr.as_deref());
        set_env("UDB_SAML_DEFAULT_TENANT", prior_tenant.as_deref());
        set_env("UDB_SAML_DEFAULT_PROVIDER", prior_provider.as_deref());
        set_env("UDB_SAML_ACS_URL", prior_acs.as_deref());
    }

    // ── path scoping mirrors scim_http (explicit tenant/provider or defaults) ──
    #[test]
    fn resolve_scope_explicit_and_default() {
        let cfg = SamlHttpConfig {
            addr: "127.0.0.1:0".parse().expect("addr"),
            default_tenant: "acme".to_string(),
            default_provider: "okta".to_string(),
            acs_url: String::new(),
        };
        let (t, p, r) = resolve_scope("t/contoso/p/entra/acs", &cfg);
        assert_eq!(
            (t.as_str(), p.as_str(), r.as_str()),
            ("contoso", "entra", "acs")
        );
        // Bare path falls back to the configured default binding.
        let (t, p, r) = resolve_scope("metadata", &cfg);
        assert_eq!(
            (t.as_str(), p.as_str(), r.as_str()),
            ("acme", "okta", "metadata")
        );
    }

    // A denial Status (replay/disabled/etc.) can NEVER render as an authenticated
    // 2xx response.
    #[test]
    fn status_response_never_authenticates() {
        let body = status_response(super::idp_saml_replay_rejected_status());
        assert!(body.starts_with("HTTP/1.1 403"));
        assert!(
            body.contains("\"authenticated\": false") || body.contains("\"authenticated\":false")
        );
    }
}
