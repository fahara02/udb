//! SCIM 2.0 (RFC 7644) HTTP/REST surface for off-the-shelf provisioners.
//!
//! Tier-7 #31. The SCIM 2.0 logic already exists as gRPC RPCs on
//! [`IdentityProviderServiceImpl`] (create/get/list/replace/patch/delete Users +
//! Groups). Standard provisioners (Okta, Microsoft Entra/Azure AD, OneLogin)
//! speak **HTTP/REST**, not gRPC, so they cannot drive the gRPC surface. This
//! module exposes the RFC-7644 HTTP endpoints and maps each request onto the
//! EXISTING gRPC handler methods — the persistence (`store::*`) and IdP event
//! emission live entirely in those handlers, so nothing is duplicated here.
//!
//! Endpoints (base `/scim/v2`):
//!   * `GET    /ServiceProviderConfig`  — discovery (capabilities)
//!   * `GET    /ResourceTypes`          — discovery (User + Group)
//!   * `GET    /Schemas`                — discovery (advertised SCIM schemas)
//!   * `POST   /Users`                  — create user
//!   * `GET    /Users`                  — list users (paged)
//!   * `GET    /Users/{id}`             — get user
//!   * `PUT    /Users/{id}`             — replace user
//!   * `PATCH  /Users/{id}`             — patch user (RFC-7644 §3.5.2)
//!   * `DELETE /Users/{id}`             — deprovision (deactivate + unlink)
//!   * `POST   /Groups`                 — create group
//!   * `GET    /Groups`                 — list groups
//!   * `GET    /Groups/{id}`            — get group
//!   * `PATCH  /Groups/{id}`            — patch group (membership → role mapping)
//!   * `DELETE /Groups/{id}`           — delete group (mapping-driven no-op)
//!
//! Transport: a raw `tokio` TcpListener with a minimal HTTP/1.1 reader, mirroring
//! [`crate::runtime::service::metrics_http_server`] (the broker has no axum/hyper
//! dependency). It is OFF by default and only binds when `UDB_SCIM_HTTP_ADDR` is
//! set, exactly like the optional ws-signalling / metrics listeners.
//!
//! Auth: each resource request resolves an enabled tenant/provider and compares
//! its bearer with that provider's encrypted, write-only `client_secret`.
//! `UDB_SCIM_BEARER_TOKEN` remains a compatibility credential only for the exact
//! `UDB_SCIM_DEFAULT_TENANT` / `UDB_SCIM_DEFAULT_PROVIDER`; it can never authorize
//! an arbitrary explicit path. Tenant and provider are resolved from the URL path
//! prefix (`/scim/v2/t/{tenant}/p/{provider}/...`) or, when omitted, from those
//! defaults so a single-tenant connector can use bare `/scim/v2/Users` paths.

use std::net::SocketAddr;
use std::sync::Arc;

use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tonic::Request;

use crate::proto::udb::core::common::v1 as common_pb;
use crate::proto::udb::core::idp::services::v1 as idp_pb;
use idp_pb::identity_provider_service_server::IdentityProviderService;

use super::IdentityProviderServiceImpl;

/// Runtime config for the SCIM HTTP listener, resolved from the environment.
///
/// Does NOT derive `Debug`: `legacy_default_bearer_token` may contain the legacy
/// environment credential. A manual `Debug` impl redacts it so a future
/// `tracing`/`{:?}` cannot spill the token into logs.
struct ScimHttpConfig {
    addr: SocketAddr,
    legacy_default_bearer_token: Option<String>,
    default_tenant: String,
    default_provider: String,
}

// 4.6 secrets posture: the legacy bearer is a long-lived provisioning credential;
// redact it in Debug while keeping the non-secret routing fields visible. A
// derived Debug would print the token verbatim — the canary test below locks
// this in (it fails if the manual impl is ever replaced by a derive).
impl std::fmt::Debug for ScimHttpConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScimHttpConfig")
            .field("addr", &self.addr)
            .field("legacy_default_bearer_token", &"[redacted]")
            .field("default_tenant", &self.default_tenant)
            .field("default_provider", &self.default_provider)
            .finish()
    }
}

impl ScimHttpConfig {
    /// Resolve from env. Returns `None` when `UDB_SCIM_HTTP_ADDR` is unset (the
    /// surface is OFF by default). Returns `None` with a warning when the addr is
    /// set. Resource requests still fail closed unless their selected active
    /// provider has a stored connector secret or the exact default scope matches
    /// the optional legacy compatibility credential.
    fn from_env() -> Option<Self> {
        let raw_addr = std::env::var("UDB_SCIM_HTTP_ADDR").ok()?;
        let addr: SocketAddr = match raw_addr.trim().parse() {
            Ok(addr) => addr,
            Err(err) => {
                tracing::warn!(value = %raw_addr, error = %err, "invalid UDB_SCIM_HTTP_ADDR; SCIM HTTP disabled");
                return None;
            }
        };
        let legacy_default_bearer_token = std::env::var("UDB_SCIM_BEARER_TOKEN")
            .ok()
            .map(|t| t.trim().to_string())
            .filter(|t| !t.is_empty());
        let default_tenant = std::env::var("UDB_SCIM_DEFAULT_TENANT")
            .unwrap_or_default()
            .trim()
            .to_string();
        let default_provider = std::env::var("UDB_SCIM_DEFAULT_PROVIDER")
            .unwrap_or_default()
            .trim()
            .to_string();
        if legacy_default_bearer_token.is_some()
            && (default_tenant.is_empty() || default_provider.is_empty())
        {
            tracing::warn!(
                "UDB_SCIM_BEARER_TOKEN is set without both SCIM defaults; the legacy token is disabled"
            );
        }
        Some(Self {
            addr,
            legacy_default_bearer_token,
            default_tenant,
            default_provider,
        })
    }
}

/// Spawn the SCIM HTTP listener bound to `shutdown`, or `None` when disabled.
/// Mirrors `SignalingServer::spawn_from_env_with_shutdown` — a detached task that
/// logs but never brings down the data plane.
pub(crate) fn spawn_from_env_with_shutdown<F>(
    service: Arc<IdentityProviderServiceImpl>,
    shutdown: F,
) -> Option<tokio::task::JoinHandle<()>>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let cfg = ScimHttpConfig::from_env()?;
    tracing::info!(addr = %cfg.addr, "SCIM 2.0 HTTP surface enabled");
    Some(tokio::spawn(async move {
        tokio::select! {
            _ = serve(service, cfg) => {}
            _ = shutdown => {
                tracing::info!("SCIM HTTP listener shutting down");
            }
        }
    }))
}

async fn serve(service: Arc<IdentityProviderServiceImpl>, cfg: ScimHttpConfig) {
    let listener = match tokio::net::TcpListener::bind(cfg.addr).await {
        Ok(l) => l,
        Err(err) => {
            tracing::warn!(addr = %cfg.addr, error = %err, "SCIM HTTP endpoint disabled");
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
                None => http_response(400, &scim_error(400, "malformed HTTP request")),
            };
            let _ = socket.write_all(response.as_bytes()).await;
        });
    }
}

/// A parsed HTTP request: method, path (no query), query string, bearer token,
/// and JSON body.
struct HttpRequest {
    method: String,
    path: String,
    query: String,
    bearer: String,
    body: String,
}

/// Read one HTTP/1.1 request with a bounded buffer + deadline (slow-loris guard),
/// mirroring the metrics listener. Reads headers, then up to `Content-Length`
/// body bytes (capped). SCIM payloads are small.
async fn read_request(socket: &mut tokio::net::TcpStream) -> Option<HttpRequest> {
    const MAX: usize = 256 * 1024;
    let mut buf: Vec<u8> = Vec::with_capacity(8192);
    let mut chunk = [0u8; 8192];
    // Read until headers complete (\r\n\r\n) or the cap is hit.
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
    let (path, query) = match raw_target.split_once('?') {
        Some((p, q)) => (p.to_string(), q.to_string()),
        None => (raw_target, String::new()),
    };

    let mut bearer = String::new();
    let mut content_length = 0usize;
    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            let name = name.trim().to_ascii_lowercase();
            let value = value.trim();
            if name == "authorization" {
                if let Some(tok) = value
                    .strip_prefix("Bearer ")
                    .or_else(|| value.strip_prefix("bearer "))
                {
                    bearer = tok.trim().to_string();
                }
            } else if name == "content-length" {
                content_length = value.parse().unwrap_or(0);
            }
        }
    }

    // Read remaining body bytes up to Content-Length.
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

    Some(HttpRequest {
        method,
        path,
        query,
        bearer,
        body,
    })
}

fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

/// Route + auth + dispatch. Returns a full HTTP response string.
async fn dispatch(
    service: &IdentityProviderServiceImpl,
    cfg: &ScimHttpConfig,
    req: HttpRequest,
) -> String {
    // Strip the `/scim/v2` prefix; everything else is "not found".
    let Some(rest) = req.path.strip_prefix("/scim/v2") else {
        return http_response(404, &scim_error(404, "not a SCIM endpoint"));
    };
    let rest = rest.trim_start_matches('/');

    // Optional tenant/provider path scoping: `t/{tenant}/p/{provider}/...`.
    let (tenant, provider, resource_path) = resolve_scope(rest, cfg);

    // Discovery endpoints are unauthenticated per RFC-7644 §4 (provisioners probe
    // them before presenting credentials).
    match (req.method.as_str(), resource_path.as_str()) {
        ("GET", "ServiceProviderConfig") => {
            return http_response(200, &service_provider_config());
        }
        ("GET", "ResourceTypes") => {
            return http_response(200, &resource_types());
        }
        ("GET", "Schemas") => {
            return http_response(200, &schemas());
        }
        _ => {}
    }

    if tenant.is_empty() || provider.is_empty() {
        return http_response(
            400,
            &scim_error(
                400,
                "tenant/provider unresolved: use /scim/v2/t/{tenant}/p/{provider}/... \
                 or set UDB_SCIM_DEFAULT_TENANT/UDB_SCIM_DEFAULT_PROVIDER",
            ),
        );
    }

    // Resolve the exact active provider before authenticating. Operational store
    // or decryption failures are service-unavailable, while a missing/disabled
    // provider is deliberately indistinguishable from a bad credential.
    let provider_secret = match service.scim_connector_secret(&tenant, &provider).await {
        Ok(Some(secret)) => secret,
        Ok(None) => {
            return http_response(401, &scim_error(401, "invalid or missing bearer token"));
        }
        Err(status) if status.code() == tonic::Code::InvalidArgument => {
            return http_response(401, &scim_error(401, "invalid or missing bearer token"));
        }
        Err(status) => {
            tracing::warn!(
                tenant = %tenant,
                provider = %provider,
                code = ?status.code(),
                "SCIM connector credential resolution failed"
            );
            return http_response(
                503,
                &scim_error(
                    503,
                    "SCIM connector authentication is temporarily unavailable",
                ),
            );
        }
    };
    let legacy = legacy_bearer_for_scope(cfg, &tenant, &provider);
    if !bearer_matches_any(&req.bearer, &provider_secret, legacy) {
        return http_response(401, &scim_error(401, "invalid or missing bearer token"));
    }

    // Split the resource path into collection + optional id.
    let mut segs = resource_path.splitn(2, '/');
    let collection = segs.next().unwrap_or_default();
    let id = segs.next().unwrap_or_default().trim_end_matches('/');

    match (req.method.as_str(), collection, id.is_empty()) {
        // ── Users ──────────────────────────────────────────────────────────
        ("POST", "Users", true) => {
            let r = idp_pb::ScimCreateUserRequest {
                tenant_id: tenant,
                provider_id: provider,
                scim_user_json: req.body,
                context: None,
            };
            match service.scim_create_user(Request::new(r)).await {
                Ok(resp) => user_response(201, resp.into_inner().user),
                Err(s) => status_response(s),
            }
        }
        ("GET", "Users", true) => {
            let r = idp_pb::ScimListUsersRequest {
                tenant_id: tenant,
                provider_id: provider,
                filter: query_param(&req.query, "filter"),
                page: Some(page_from_query(&req.query)),
            };
            match service.scim_list_users(Request::new(r)).await {
                Ok(resp) => {
                    let inner = resp.into_inner();
                    let total = inner
                        .page
                        .map(|p| p.total_count)
                        .unwrap_or(inner.users.len() as i64);
                    list_response(inner.users.iter().map(user_to_json).collect(), total)
                }
                Err(s) => status_response(s),
            }
        }
        ("GET", "Users", false) => {
            let r = idp_pb::ScimGetUserRequest {
                tenant_id: tenant,
                provider_id: provider,
                scim_user_id: id.to_string(),
            };
            match service.scim_get_user(Request::new(r)).await {
                Ok(resp) => user_response(200, resp.into_inner().user),
                Err(s) => status_response(s),
            }
        }
        ("PUT", "Users", false) => {
            let r = idp_pb::ScimReplaceUserRequest {
                tenant_id: tenant,
                provider_id: provider,
                scim_user_id: id.to_string(),
                scim_user_json: req.body,
                context: None,
            };
            match service.scim_replace_user(Request::new(r)).await {
                Ok(resp) => user_response(200, resp.into_inner().user),
                Err(s) => status_response(s),
            }
        }
        ("PATCH", "Users", false) => {
            let ops = match parse_patch_ops(&req.body) {
                Ok(ops) => ops,
                Err(msg) => return http_response(400, &scim_error(400, &msg)),
            };
            let r = idp_pb::ScimPatchUserRequest {
                tenant_id: tenant,
                provider_id: provider,
                scim_user_id: id.to_string(),
                operations: ops,
                context: None,
            };
            match service.scim_patch_user(Request::new(r)).await {
                Ok(resp) => user_response(200, resp.into_inner().user),
                Err(s) => status_response(s),
            }
        }
        ("DELETE", "Users", false) => {
            let r = idp_pb::ScimDeleteUserRequest {
                tenant_id: tenant,
                provider_id: provider,
                scim_user_id: id.to_string(),
                context: None,
            };
            match service.scim_delete_user(Request::new(r)).await {
                // RFC-7644 §3.6: a successful DELETE returns 204 No Content.
                Ok(_) => http_no_content(),
                Err(s) => status_response(s),
            }
        }
        // ── Groups ─────────────────────────────────────────────────────────
        ("POST", "Groups", true) => {
            let r = idp_pb::ScimCreateGroupRequest {
                tenant_id: tenant,
                provider_id: provider,
                scim_group_json: req.body,
                context: None,
            };
            match service.scim_create_group(Request::new(r)).await {
                Ok(resp) => group_response(201, resp.into_inner().group),
                Err(s) => status_response(s),
            }
        }
        ("GET", "Groups", true) => {
            let r = idp_pb::ScimListGroupsRequest {
                tenant_id: tenant,
                provider_id: provider,
                filter: query_param(&req.query, "filter"),
                page: Some(page_from_query(&req.query)),
            };
            match service.scim_list_groups(Request::new(r)).await {
                Ok(resp) => {
                    let inner = resp.into_inner();
                    let total = inner
                        .page
                        .map(|p| p.total_count)
                        .unwrap_or(inner.groups.len() as i64);
                    list_response(inner.groups.iter().map(group_to_json).collect(), total)
                }
                Err(s) => status_response(s),
            }
        }
        ("GET", "Groups", false) => {
            let r = idp_pb::ScimGetGroupRequest {
                tenant_id: tenant,
                provider_id: provider,
                scim_group_id: id.to_string(),
            };
            match service.scim_get_group(Request::new(r)).await {
                Ok(resp) => group_response(200, resp.into_inner().group),
                Err(s) => status_response(s),
            }
        }
        ("PATCH", "Groups", false) => {
            let ops = match parse_patch_ops(&req.body) {
                Ok(ops) => ops,
                Err(msg) => return http_response(400, &scim_error(400, &msg)),
            };
            let r = idp_pb::ScimPatchGroupRequest {
                tenant_id: tenant,
                provider_id: provider,
                scim_group_id: id.to_string(),
                operations: ops,
                context: None,
            };
            match service.scim_patch_group(Request::new(r)).await {
                Ok(resp) => group_response(200, resp.into_inner().group),
                Err(s) => status_response(s),
            }
        }
        ("DELETE", "Groups", false) => {
            let r = idp_pb::ScimDeleteGroupRequest {
                tenant_id: tenant,
                provider_id: provider,
                scim_group_id: id.to_string(),
                context: None,
            };
            match service.scim_delete_group(Request::new(r)).await {
                Ok(_) => http_no_content(),
                Err(s) => status_response(s),
            }
        }
        _ => http_response(404, &scim_error(404, "unknown SCIM resource or method")),
    }
}

/// Resolve `(tenant, provider, resource_path)` from the path remainder.
/// Supports the explicit `t/{tenant}/p/{provider}/<resource>` form, falling back
/// to the configured defaults for the bare `/scim/v2/<resource>` form.
fn resolve_scope(rest: &str, cfg: &ScimHttpConfig) -> (String, String, String) {
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

/// Length-checked, byte-wise bearer comparison (avoids early-exit timing leak).
fn bearer_matches(presented: &str, expected: &str) -> bool {
    let a = presented.as_bytes();
    let b = expected.as_bytes();
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

fn legacy_bearer_for_scope<'a>(
    cfg: &'a ScimHttpConfig,
    tenant: &str,
    provider: &str,
) -> Option<&'a str> {
    if !cfg.default_tenant.is_empty()
        && !cfg.default_provider.is_empty()
        && tenant == cfg.default_tenant
        && provider == cfg.default_provider
    {
        cfg.legacy_default_bearer_token.as_deref()
    } else {
        None
    }
}

fn bearer_matches_any(presented: &str, provider_secret: &str, legacy: Option<&str>) -> bool {
    !presented.is_empty()
        && ((!provider_secret.is_empty() && bearer_matches(presented, provider_secret))
            || legacy.is_some_and(|expected| bearer_matches(presented, expected)))
}

/// Translate the SCIM gRPC `ScimPatchUserRequest`/`ScimPatchGroupRequest` op list
/// from an RFC-7644 PATCH body (`{"Operations":[{op,path,value}]}`).
fn parse_patch_ops(body: &str) -> Result<Vec<idp_pb::ScimPatchOp>, String> {
    let v: Value =
        serde_json::from_str(body.trim()).map_err(|e| format!("invalid PATCH body JSON: {e}"))?;
    let ops = v
        .get("Operations")
        .or_else(|| v.get("operations"))
        .and_then(Value::as_array)
        .ok_or_else(|| "PATCH body missing 'Operations' array".to_string())?;
    Ok(ops
        .iter()
        .map(|o| idp_pb::ScimPatchOp {
            op: o
                .get("op")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            path: o
                .get("path")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            // `value` may be any JSON type; the gRPC handler expects a JSON string.
            value_json: o
                .get("value")
                .map(|val| val.to_string())
                .unwrap_or_else(|| "null".to_string()),
        })
        .collect())
}

fn query_param(query: &str, key: &str) -> String {
    for pair in query.split('&') {
        if let Some((k, v)) = pair.split_once('=') {
            if k == key {
                return urlencoding::decode(v)
                    .map(|c| c.into_owned())
                    .unwrap_or_default();
            }
        }
    }
    String::new()
}

/// Map SCIM `startIndex` (1-based) + `count` to the proto `PageRequest`
/// (`page` 1-based, `page_size`).
fn page_from_query(query: &str) -> common_pb::PageRequest {
    let count = query_param(query, "count").parse::<i32>().unwrap_or(0);
    let start_index = query_param(query, "startIndex")
        .parse::<i32>()
        .unwrap_or(1)
        .max(1);
    let page_size = if count > 0 { count } else { 0 };
    let page = if page_size > 0 {
        ((start_index - 1) / page_size) + 1
    } else {
        1
    };
    common_pb::PageRequest {
        page,
        page_size,
        page_token: String::new(),
    }
}

// ── SCIM JSON serialization ───────────────────────────────────────────────────

/// Render a `ScimUser` as the canonical SCIM JSON resource. Reuses the proto's
/// `raw_json` (built by `scim_user_pb`) when present, else assembles from fields.
fn user_to_json(u: &idp_pb::ScimUser) -> Value {
    if !u.raw_json.trim().is_empty() {
        if let Ok(v) = serde_json::from_str::<Value>(&u.raw_json) {
            return v;
        }
    }
    json!({
        "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
        "id": u.id,
        "userName": u.user_name,
        "displayName": u.display_name,
        "active": u.active,
        "emails": [{ "value": u.email, "primary": true }],
    })
}

fn group_to_json(g: &idp_pb::ScimGroup) -> Value {
    if !g.raw_json.trim().is_empty() {
        if let Ok(v) = serde_json::from_str::<Value>(&g.raw_json) {
            return v;
        }
    }
    json!({
        "schemas": ["urn:ietf:params:scim:schemas:core:2.0:Group"],
        "id": g.id,
        "displayName": g.display_name,
        "members": g.members,
    })
}

fn user_response(status: u16, user: Option<idp_pb::ScimUser>) -> String {
    match user {
        Some(u) => {
            // RFC-7644 §3.1: a 201 Created must carry a Location header.
            let location =
                (status == 201 && !u.id.is_empty()).then(|| format!("/scim/v2/Users/{}", u.id));
            http_response_with_location(status, &user_to_json(&u).to_string(), location)
        }
        None => http_response(500, &scim_error(500, "empty SCIM user response")),
    }
}

fn group_response(status: u16, group: Option<idp_pb::ScimGroup>) -> String {
    match group {
        Some(g) => {
            // RFC-7644 §3.3: a 201 Created must carry a Location header.
            let location =
                (status == 201 && !g.id.is_empty()).then(|| format!("/scim/v2/Groups/{}", g.id));
            http_response_with_location(status, &group_to_json(&g).to_string(), location)
        }
        None => http_response(500, &scim_error(500, "empty SCIM group response")),
    }
}

/// RFC-7644 §3.4.2 ListResponse envelope.
fn list_response(resources: Vec<Value>, total: i64) -> String {
    let body = json!({
        "schemas": ["urn:ietf:params:scim:api:messages:2.0:ListResponse"],
        "totalResults": total,
        "startIndex": 1,
        "itemsPerPage": resources.len(),
        "Resources": resources,
    });
    http_response(200, &body.to_string())
}

/// Map a tonic `Status` to the closest SCIM HTTP status + RFC-7644 §3.12 error.
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
    http_response(http, &scim_error(http, status.message()))
}

fn scim_error(status: u16, detail: &str) -> String {
    json!({
        "schemas": ["urn:ietf:params:scim:api:messages:2.0:Error"],
        "status": status.to_string(),
        "detail": detail,
    })
    .to_string()
}

/// RFC-7644 §4 ServiceProviderConfig — advertises the implemented capabilities so
/// provisioners configure themselves correctly.
fn service_provider_config() -> String {
    json!({
        "schemas": ["urn:ietf:params:scim:schemas:core:2.0:ServiceProviderConfig"],
        "documentationUri": "https://github.com/fahara02/udb",
        "patch": { "supported": true },
        "bulk": { "supported": false, "maxOperations": 0, "maxPayloadSize": 0 },
        "filter": { "supported": false, "maxResults": 0 },
        "changePassword": { "supported": false },
        "sort": { "supported": false },
        "etag": { "supported": false },
        "authenticationSchemes": [{
            "type": "oauthbearertoken",
            "name": "OAuth Bearer Token",
            "description": "Static bearer token presented in the Authorization header.",
            "primary": true
        }]
    })
    .to_string()
}

/// RFC-7644 §4 ResourceTypes — the User + Group resources this surface exposes.
fn resource_types() -> String {
    json!([
        {
            "schemas": ["urn:ietf:params:scim:schemas:core:2.0:ResourceType"],
            "id": "User",
            "name": "User",
            "endpoint": "/Users",
            "schema": "urn:ietf:params:scim:schemas:core:2.0:User"
        },
        {
            "schemas": ["urn:ietf:params:scim:schemas:core:2.0:ResourceType"],
            "id": "Group",
            "name": "Group",
            "endpoint": "/Groups",
            "schema": "urn:ietf:params:scim:schemas:core:2.0:Group"
        }
    ])
    .to_string()
}

fn schemas() -> String {
    json!([
        { "id": "urn:ietf:params:scim:schemas:core:2.0:User", "name": "User" },
        { "id": "urn:ietf:params:scim:schemas:core:2.0:Group", "name": "Group" }
    ])
    .to_string()
}

// ── raw HTTP/1.1 response helpers ─────────────────────────────────────────────

fn http_response(status: u16, body: &str) -> String {
    http_response_with_location(status, body, None)
}

/// Like [`http_response`] but emits a `Location` header (RFC-7644 §3.1/§3.3) when
/// `location` is set — used for 201 Created responses pointing at the new resource.
fn http_response_with_location(status: u16, body: &str, location: Option<String>) -> String {
    let reason = match status {
        200 => "OK",
        201 => "Created",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        409 => "Conflict",
        500 => "Internal Server Error",
        503 => "Service Unavailable",
        _ => "OK",
    };
    let mut head = format!(
        "HTTP/1.1 {status} {reason}\r\ncontent-type: application/scim+json; charset=utf-8\r\ncontent-length: {}\r\nconnection: close\r\n",
        body.len()
    );
    // RFC-7644 §3.3: a 401 must carry a WWW-Authenticate challenge.
    if status == 401 {
        head.push_str("www-authenticate: Bearer\r\n");
    }
    // RFC-7644 §3.1/§3.3: a 201 Created points at the new resource via Location.
    if let Some(loc) = location {
        head.push_str(&format!("location: {loc}\r\n"));
    }
    head.push_str("\r\n");
    head.push_str(body);
    head
}

fn http_no_content() -> String {
    "HTTP/1.1 204 No Content\r\ncontent-length: 0\r\nconnection: close\r\n\r\n".to_string()
}

#[cfg(test)]
mod scim_http_redaction_tests {
    use super::*;

    // 4.6 secrets posture canary: a `{:?}` of ScimHttpConfig must never spill the
    // bearer token. Mirrors the dsn.rs / encryption.rs redaction canaries.
    #[test]
    fn scim_http_config_debug_never_leaks_bearer_token() {
        let cfg = ScimHttpConfig {
            addr: "127.0.0.1:9999".parse().expect("valid addr"),
            legacy_default_bearer_token: Some("udb-canary-SCIM-SECRET".to_string()),
            default_tenant: "acme".to_string(),
            default_provider: "okta".to_string(),
        };
        let dbg = format!("{cfg:?}");
        assert!(
            !dbg.contains("udb-canary-SCIM-SECRET"),
            "ScimHttpConfig Debug leaked the bearer token: {dbg}"
        );
        assert!(
            dbg.contains("[redacted]"),
            "expected redaction marker: {dbg}"
        );
        // Non-secret routing fields stay visible (don't over-redact).
        assert!(
            dbg.contains("acme"),
            "default_tenant should remain visible: {dbg}"
        );
        assert!(
            dbg.contains("okta"),
            "default_provider should remain visible: {dbg}"
        );
    }

    #[test]
    fn legacy_bearer_is_bound_to_exact_default_connector() {
        let cfg = ScimHttpConfig {
            addr: "127.0.0.1:9999".parse().expect("valid addr"),
            legacy_default_bearer_token: Some("connector-a-token".to_string()),
            default_tenant: "tenant-a".to_string(),
            default_provider: "provider-a".to_string(),
        };

        let default_legacy = legacy_bearer_for_scope(&cfg, "tenant-a", "provider-a");
        assert!(bearer_matches_any(
            "connector-a-token",
            "provider-a-secret",
            default_legacy
        ));
        assert!(legacy_bearer_for_scope(&cfg, "tenant-a", "provider-b").is_none());
        assert!(legacy_bearer_for_scope(&cfg, "tenant-b", "provider-a").is_none());
        assert!(!bearer_matches_any(
            "connector-a-token",
            "provider-b-secret",
            legacy_bearer_for_scope(&cfg, "tenant-a", "provider-b")
        ));
    }
}
