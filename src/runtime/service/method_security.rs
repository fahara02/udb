//! Phase 1 (per-RPC auth enforcement).
//!
//! The native control-plane listener historically applied one broad bearer +
//! `udb:admin` check to every RPC, ignoring the `endpoint_security` annotations
//! the protos already carry. This module makes those annotations the source of
//! truth at runtime:
//!
//!   * [`method_security_registry`] decodes the embedded `FileDescriptorSet` and
//!     extracts the `udb.core.common.v1.endpoint_security` option (extension
//!     field 51001) for every RPC, keyed by its gRPC path
//!     (`/<package>.<Service>/<Method>`).
//!   * [`MethodSecurityLayer`] enforces it as a tower layer.
//!
//! Why a tower layer and not a tonic `Interceptor`: an `Interceptor` only sees a
//! request's metadata/extensions — tonic strips the URI *before* calling it
//! (`InterceptedService::call`), so it cannot tell which RPC is being invoked. A
//! tower `Service` sees the raw `http::Request`, whose `uri().path()` is exactly
//! the `/<package>.<Service>/<Method>` key we need.
//!
//! Posture (non-breaking hardening): `AUTH_MODE_PUBLIC` bootstrap RPCs become
//! reachable without the control-plane admin token (so a gateway can forward
//! `Authenticate`/`Login`/`GetJwks`/forgot+reset/WebAuthn start+finish without
//! minting one). Every other RPC — annotated non-public, or unannotated (fail
//! closed) — still requires the admin/auth-admin scope exactly as before, plus
//! any method-declared scope/role and tenant-claim consistency on top.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use tonic::Status;
use tonic::body::BoxBody;
use tonic::codegen::http;
use tonic::codegen::{Context, Future, Pin, Poll, Service};

use crate::metrics::MetricsRecorder;
use crate::runtime::descriptor_manifest::{
    EndpointSecurityContract, descriptor_contract_manifest_static,
};
use crate::runtime::security::{SecurityClaims, SecurityConfig, validate_bearer_token};

/// Mirror of `udb.core.common.v1.AuthMode`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthMode {
    Unspecified,
    Public,
    Bearer,
    ApiKey,
    ServiceAccount,
}

impl AuthMode {
    fn from_i32(value: i32) -> Self {
        match value {
            1 => Self::Public,
            2 => Self::Bearer,
            3 => Self::ApiKey,
            4 => Self::ServiceAccount,
            _ => Self::Unspecified,
        }
    }
}

/// Decoded `endpoint_security` for a single RPC.
///
/// Fields enforced at this transport layer. Message-body field extraction stays
/// in handlers/generated mappers, but every check possible from gRPC metadata is
/// centralized here so descriptor security fails closed before a native handler
/// sees the request.
#[derive(Clone, Debug)]
pub struct MethodSecurity {
    pub mode: AuthMode,
    pub roles: Vec<String>,
    pub scopes: Vec<String>,
    pub tenant_required: bool,
    pub csrf_required: bool,
    pub internal_grpc_only: bool,
    pub allowed_credential_types: Vec<i32>,
    pub tenant_field: String,
    pub project_field: String,
    pub request_context_required: bool,
    /// `endpoint_security.policy_ref` — the named policy set this RPC's per-action
    /// authorization should be evaluated against (decoded from the descriptor;
    /// `None`/empty when the annotation left it unset). Carried so the native
    /// authz DECISION ENGINE can be invoked per action (D2-full), not just the
    /// coarse transport scope gate.
    pub policy_ref: Option<String>,
    /// `endpoint_security.decision_resource` — the explicit resource string to
    /// authorize the action against. When unset the handler synthesizes
    /// `native.rpc:{service}/{method}` so a decision is still recorded.
    pub decision_resource: Option<String>,
    /// `endpoint_security.required_assurance_level` — the minimum authentication
    /// assurance level (AAL/MFA step-up) required to invoke this RPC. `0` = unset
    /// (no step-up requirement). Enforced against the bearer's `acr` claim (C21).
    pub required_assurance_level: i32,
    /// `endpoint_security.owner_field` — the body field naming the principal that
    /// owns the target resource. Enforced via
    /// [`enforce_body_owner_matches_claim`] when a handler extracts it (C23).
    pub owner_field: Option<String>,
    /// `endpoint_security.rate_limit_policy_ref` — the named rate-limit policy this
    /// RPC's abuse bucket is keyed under, so distinct policies get distinct buckets
    /// and an operator can tune a named policy's ceiling (C24).
    pub rate_limit_policy_ref: Option<String>,
    /// `endpoint_security.audit_event_type` — the per-RPC audit event type to
    /// stamp instead of a synthesized one (C25).
    pub audit_event_type: Option<String>,
}

/// Normalize a descriptor string field into `Some(non-empty)` / `None`.
fn opt_non_empty(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

/// Map an `acr` claim string (e.g. `aal1`, `aal2`) to its numeric assurance
/// level. Unknown/absent forms are the baseline level 1 (C21).
fn aal_level(acr: &str) -> i32 {
    acr.trim()
        .to_ascii_lowercase()
        .strip_prefix("aal")
        .and_then(|n| n.parse::<i32>().ok())
        .filter(|n| *n >= 1)
        .unwrap_or(1)
}

/// Project a decoded `endpoint_security` contract into the enforced
/// [`MethodSecurity`]. This is the ONLY runtime copy on the request path, so any
/// field NOT projected here is unenforceable by construction — the seam that made
/// C21/C23/C24/C25 decorative. L2's inventory test pins that every contract field
/// is either projected here (enforced) or documented as metadata-only.
fn method_security_from_contract(es: &EndpointSecurityContract) -> MethodSecurity {
    MethodSecurity {
        mode: AuthMode::from_i32(es.mode),
        roles: es.roles.clone(),
        scopes: es.scopes.clone(),
        tenant_required: es.tenant_required,
        csrf_required: es.csrf_required,
        internal_grpc_only: es.internal_grpc_only,
        allowed_credential_types: es.allowed_credential_types.clone(),
        tenant_field: es.tenant_field.clone(),
        project_field: es.project_field.clone(),
        request_context_required: es.request_context_required,
        policy_ref: opt_non_empty(&es.policy_ref),
        decision_resource: opt_non_empty(&es.decision_resource),
        required_assurance_level: es.required_assurance_level,
        owner_field: opt_non_empty(&es.owner_field),
        rate_limit_policy_ref: opt_non_empty(&es.rate_limit_policy_ref),
        audit_event_type: opt_non_empty(&es.audit_event_type),
    }
}

pub(crate) fn build_registry() -> HashMap<String, MethodSecurity> {
    let mut map = HashMap::new();
    let manifest = descriptor_contract_manifest_static();
    for service in &manifest.services {
        for method in &service.methods {
            let Some(es) = method.endpoint_security.as_ref() else {
                continue;
            };
            map.insert(method.grpc_path(), method_security_from_contract(es));
        }
    }
    map
}

/// The process-wide method → security map, decoded once from the embedded proto
/// descriptor (proto is the single source of truth).
pub fn method_security_registry() -> &'static HashMap<String, MethodSecurity> {
    static REGISTRY: OnceLock<HashMap<String, MethodSecurity>> = OnceLock::new();
    REGISTRY.get_or_init(build_registry)
}

/// Look up the declared security for a gRPC path (`/<package>.<Service>/<Method>`).
pub fn method_security(path: &str) -> Option<&'static MethodSecurity> {
    method_security_registry().get(path)
}

/// Control-plane scopes that authorize any non-public native RPC.
const ADMIN_SCOPES: [&str; 4] = ["*", "udb:*", "udb:admin", "udb:auth:admin"];

/// Roles/scopes that explicitly carry platform authority even when the verified
/// identity is tenant/project-bound. Broad control-plane scopes stay sufficient
/// for action authorization, but they are not an authority to erase a concrete
/// tenant/project claim.
const PLATFORM_ADMIN_ROLES: [&str; 4] = [
    "platform_admin",
    "udb:platform_admin",
    "superadmin",
    "super_admin",
];
const PLATFORM_ADMIN_SCOPES: [&str; 1] = ["udb:platform_admin"];

/// Whether a role token is reserved for explicit platform authority.  These
/// tokens are not ordinary tenant-defined role names: once present in a
/// verified claim they authorize cross-tenant/project operations.
pub(crate) fn is_platform_authority_role(role: &str) -> bool {
    let role = role.trim();
    PLATFORM_ADMIN_ROLES
        .iter()
        .any(|admin| role.eq_ignore_ascii_case(admin))
}

/// Whether a scope token is reserved for explicit platform authority.
pub(crate) fn is_platform_authority_scope(scope: &str) -> bool {
    let scope = scope.trim();
    PLATFORM_ADMIN_SCOPES
        .iter()
        .any(|admin| scope.eq_ignore_ascii_case(admin))
}

/// Canonical cross-tenant/project authority predicate shared by the method
/// security layer and native auth handlers.
///
/// Explicit platform roles/scopes always authorize the boundary crossing. The
/// legacy broad control-plane scopes authorize it only for a deliberately
/// unbound operator identity; once a credential carries a tenant or project,
/// those scopes remain action privileges inside that claim boundary.
pub(crate) fn has_cross_tenant_platform_authority(
    tenant_id: &str,
    project_id: &str,
    scopes: &[String],
    roles: &[String],
) -> bool {
    let explicit_role = roles.iter().any(|role| is_platform_authority_role(role));
    let explicit_scope = scopes
        .iter()
        .any(|scope| is_platform_authority_scope(scope));
    if explicit_role || explicit_scope {
        return true;
    }

    tenant_id.trim().is_empty()
        && project_id.trim().is_empty()
        && scopes
            .iter()
            .any(|scope| ADMIN_SCOPES.contains(&scope.trim()))
}

fn header_str<'a>(headers: &'a http::HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|value| value.to_str().ok())
}

fn non_empty_header<'a>(headers: &'a http::HeaderMap, name: &str) -> Option<&'a str> {
    header_str(headers, name)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn project_header(headers: &http::HeaderMap) -> Option<&str> {
    non_empty_header(headers, "x-udb-project-id")
        .or_else(|| non_empty_header(headers, "x-project-id"))
}

fn header_truthy(headers: &http::HeaderMap, name: &str) -> bool {
    matches!(
        header_str(headers, name)
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "1" | "true" | "yes" | "on"
    )
}

fn bearer_token(headers: &http::HeaderMap) -> Option<&str> {
    header_str(headers, "authorization").and_then(|header| header.strip_prefix("Bearer "))
}

fn authorization_scheme(headers: &http::HeaderMap) -> Option<String> {
    header_str(headers, "authorization")
        .and_then(|header| header.split_once(' ').map(|(scheme, _)| scheme))
        .map(|scheme| scheme.trim().to_ascii_lowercase())
        .filter(|scheme| !scheme.is_empty())
}

fn direct_credential_type(headers: &http::HeaderMap) -> Option<CredentialType> {
    match authorization_scheme(headers).as_deref() {
        Some("apikey" | "api-key") => return Some(CredentialType::ApiKey),
        Some("session") => return Some(CredentialType::Session),
        _ => {}
    }
    if non_empty_header(headers, "x-api-key")
        .or_else(|| non_empty_header(headers, "x-udb-api-key"))
        .is_some()
    {
        return Some(CredentialType::ApiKey);
    }
    if non_empty_header(headers, "x-udb-session")
        .or_else(|| non_empty_header(headers, "x-udb-session-id"))
        .or_else(|| non_empty_header(headers, "x-session-id"))
        .is_some()
    {
        return Some(CredentialType::Session);
    }
    None
}

fn declared_allows_credential(security: &MethodSecurity, credential: CredentialType) -> bool {
    security.allowed_credential_types.is_empty()
        || security
            .allowed_credential_types
            .contains(&(credential as i32))
}

fn credential_type_for_bearer_claims(claims: &SecurityClaims) -> CredentialType {
    match claims
        .auth_method
        .as_deref()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "api_key" | "apikey" | "api-key" => CredentialType::ApiKey,
        "session" => CredentialType::Session,
        "service" | "service_account" | "service-account" | "client_credentials" => {
            CredentialType::ServiceAccount
        }
        _ if claims
            .service_identity
            .as_deref()
            .unwrap_or_default()
            .trim()
            .is_empty() =>
        {
            CredentialType::BearerJwt
        }
        _ => CredentialType::ServiceAccount,
    }
}

fn enforce_direct_credential_type(
    declared: Option<&MethodSecurity>,
    credential: CredentialType,
) -> Result<(), (Status, &'static str)> {
    if let Some(security) = declared
        && !declared_allows_credential(security, credential)
    {
        return Err((
            method_security_policy_denied(
                deny_reason::CREDENTIAL_TYPE,
                format!(
                    "{} credential is not allowed for this method",
                    credential.label()
                ),
            ),
            deny_reason::CREDENTIAL_TYPE,
        ));
    }
    Err((
        Status::unauthenticated(format!(
            "{} credential must be exchanged for a validated bearer JWT before native control-plane RPCs",
            credential.label()
        )),
        deny_reason::CREDENTIAL_TYPE,
    ))
}

fn enforce_bearer_credential_type(
    declared: Option<&MethodSecurity>,
    claims: &SecurityClaims,
) -> Result<(), (Status, &'static str)> {
    let Some(security) = declared else {
        return Ok(());
    };
    if security.allowed_credential_types.is_empty() {
        return Ok(());
    }
    let credential = credential_type_for_bearer_claims(claims);
    if declared_allows_credential(security, credential) {
        return Ok(());
    }
    Err((
        method_security_policy_denied(
            deny_reason::CREDENTIAL_TYPE,
            format!(
                "{} credential is not allowed for this method",
                credential.label()
            ),
        ),
        deny_reason::CREDENTIAL_TYPE,
    ))
}

/// Reason labels for `udb_method_security_denials_total{reason}` (Phase 10).
/// Bounded set so the metric stays low-cardinality.
mod deny_reason {
    pub const DISABLED: &str = "service_disabled";
    pub const PUBLIC_CRED: &str = "public_credential";
    pub const MISSING_BEARER: &str = "missing_bearer";
    pub const INVALID_BEARER: &str = "invalid_bearer";
    /// The credential could not be validated because the durable auth store was
    /// unavailable (a dependency outage, not a credential rejection). Surfaced as
    /// a retryable UNAVAILABLE, not an auth denial (UDB-DB-READINESS-001).
    pub const STORE_UNAVAILABLE: &str = "store_unavailable";
    pub const CREDENTIAL_STATE: &str = "credential_state";
    pub const CREDENTIAL_TYPE: &str = "credential_type";
    pub const SCOPE: &str = "scope";
    pub const ROLE: &str = "role";
    /// The bearer's authentication assurance level (AAL/MFA) is below the level
    /// the RPC declares via `required_assurance_level` (C21 step-up gate).
    pub const ASSURANCE: &str = "assurance_level";
    /// The decoded body's owner field does not match the authenticated principal
    /// (C23 ownership guard).
    pub const OWNER: &str = "owner_mismatch";
    pub const INTERNAL_ONLY: &str = "internal_only";
    pub const CSRF: &str = "csrf";
    pub const REQUEST_CONTEXT: &str = "request_context";
    pub const TENANT_REQUIRED: &str = "tenant_required";
    pub const TENANT_MISMATCH: &str = "tenant_mismatch";
    pub const PROJECT_MISMATCH: &str = "project_mismatch";
    pub const PROJECT_REQUIRED: &str = "project_required";
    pub const PUBLIC_RATE_LIMIT: &str = "public_rate_limit";
    /// The caller authenticated, but its OWN claim tenant has been observed
    /// SUSPENDED/INACTIVE on this node — live tokens are revoked at the request
    /// gate before dispatch instead of waiting for the bearer TTL.
    pub const TENANT_SUSPENDED: &str = "tenant_suspended";
}

fn method_security_policy_denied(reason: &'static str, message: impl Into<String>) -> Status {
    crate::runtime::executor_utils::policy_status_with_code(
        tonic::Code::PermissionDenied,
        "method_security",
        reason,
        message,
    )
}

/// Default per-caller-per-minute ceiling for AUTH_MODE_PUBLIC bootstrap RPCs.
/// Override: `UDB_PUBLIC_BOOTSTRAP_RATE_LIMIT_PER_MINUTE` (positive integer).
const DEFAULT_PUBLIC_BOOTSTRAP_RATE_LIMIT_PER_MINUTE: u32 = 60;

/// Parse an override for the public-bootstrap rate limit; invalid/zero/unset
/// values fall back to the named default (fail to a sane limit, never to ∞).
fn parse_public_bootstrap_rate_limit(value: Option<&str>) -> u32 {
    value
        .and_then(|raw| raw.trim().parse::<u32>().ok())
        .filter(|&limit| limit > 0)
        .unwrap_or(DEFAULT_PUBLIC_BOOTSTRAP_RATE_LIMIT_PER_MINUTE)
}

fn public_bootstrap_rate_limit_per_minute() -> u32 {
    static LIMIT: OnceLock<u32> = OnceLock::new();
    *LIMIT.get_or_init(|| {
        parse_public_bootstrap_rate_limit(
            std::env::var("UDB_PUBLIC_BOOTSTRAP_RATE_LIMIT_PER_MINUTE")
                .ok()
                .as_deref(),
        )
    })
}

/// Normalize a `rate_limit_policy_ref` into its env override key
/// `UDB_RATE_LIMIT_POLICY_<REF>` (uppercased; non-alphanumerics → `_`).
fn rate_limit_policy_env_key(policy_ref: &str) -> String {
    format!(
        "UDB_RATE_LIMIT_POLICY_{}",
        policy_ref
            .to_ascii_uppercase()
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
            .collect::<String>()
    )
}

/// Resolve the per-minute ceiling for a named `rate_limit_policy_ref` (C24). A
/// declared policy can be tuned via its `UDB_RATE_LIMIT_POLICY_<REF>` env var;
/// unset falls back to the global public-bootstrap limit. Memoized per ref so the
/// env is read once per policy, not per request.
fn public_bootstrap_rate_limit_for(policy_ref: Option<&str>) -> u32 {
    let Some(reference) = policy_ref.map(str::trim).filter(|r| !r.is_empty()) else {
        return public_bootstrap_rate_limit_per_minute();
    };
    static CACHE: OnceLock<Mutex<HashMap<String, u32>>> = OnceLock::new();
    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let Ok(mut map) = cache.lock() else {
        return public_bootstrap_rate_limit_per_minute();
    };
    if let Some(limit) = map.get(reference) {
        return *limit;
    }
    let env_key = rate_limit_policy_env_key(reference);
    let limit = std::env::var(&env_key)
        .ok()
        .as_deref()
        .and_then(|raw| raw.trim().parse::<u32>().ok())
        .filter(|&l| l > 0)
        .unwrap_or_else(public_bootstrap_rate_limit_per_minute);
    map.insert(reference.to_string(), limit);
    limit
}

/// Whether a trusted gateway in front of UDB sets `x-forwarded-for`/`x-real-ip`.
/// OFF by default: any client can forge those headers, so without a declared
/// trusted proxy they must never select the rate-limit bucket (a forger could
/// otherwise mint itself a fresh bucket per request and bypass the limit).
/// Env: `UDB_TRUST_PROXY_IP_HEADERS` (truthy: 1/true/yes/on).
fn trust_proxy_ip_headers() -> bool {
    static TRUST: OnceLock<bool> = OnceLock::new();
    *TRUST.get_or_init(|| {
        matches!(
            std::env::var("UDB_TRUST_PROXY_IP_HEADERS")
                .unwrap_or_default()
                .trim()
                .to_ascii_lowercase()
                .as_str(),
            "1" | "true" | "yes" | "on"
        )
    })
}

/// Transport-level peer facts for the current request, read from the
/// connect-info extensions tonic's TCP/TLS acceptors install on every request
/// served through `Server::serve*`. Both are absent only for non-transport
/// invocations (in-process/test harness), where they fail closed.
#[derive(Clone, Copy, Debug, Default)]
struct TransportPeer {
    /// Peer socket address from `TcpConnectInfo`.
    remote_addr: Option<std::net::SocketAddr>,
    /// True when the TLS handshake carried a client certificate chain that
    /// rustls verified against the configured client CA — a real mTLS identity,
    /// not a header any client could set.
    mtls_client_identity: bool,
}

fn transport_peer(extensions: &http::Extensions) -> TransportPeer {
    use tonic::transport::server::{TcpConnectInfo, TlsConnectInfo};
    if let Some(tls) = extensions.get::<TlsConnectInfo<TcpConnectInfo>>() {
        return TransportPeer {
            remote_addr: tls.get_ref().remote_addr(),
            mtls_client_identity: tls
                .peer_certs()
                .map(|certs| !certs.is_empty())
                .unwrap_or(false),
        };
    }
    TransportPeer {
        remote_addr: extensions
            .get::<TcpConnectInfo>()
            .and_then(|info| info.remote_addr()),
        mtls_client_identity: false,
    }
}

/// Real `internal_grpc_only` gate: the request must originate from a loopback
/// peer (the in-host internal listener) OR carry a TLS-verified mTLS client
/// identity. Both facts come from the transport's connect-info extensions —
/// never from headers, which any client can forge (the old content-type check
/// passed for every gRPC client). No resolvable peer (in-process invocation
/// without connect info) fails CLOSED.
fn internal_peer_allowed(peer: &TransportPeer) -> bool {
    peer.mtls_client_identity
        || peer
            .remote_addr
            .map(|addr| addr.ip().is_loopback())
            .unwrap_or(false)
}

/// Fail-closed `internal_grpc_only` gate. An RPC whose descriptor sets
/// `internal_grpc_only` (e.g. the control-plane xDS push streams that only a
/// data-plane node should open) is reachable solely by an internal peer — a
/// loopback caller or one carrying a verified mTLS client identity. Any other
/// caller is rejected. A method that does not set the flag (or an absent
/// declaration) is not gated here; the rest of `enforce` still applies its
/// bearer/scope/role checks.
///
/// This is the single enforcement point, factored out of [`enforce`] (its only
/// production caller) so the gate is unit-testable directly against a declared
/// [`MethodSecurity`] without depending on a regenerated descriptor.
fn enforce_internal_grpc_only(
    declared: Option<&MethodSecurity>,
    peer: &TransportPeer,
) -> Result<(), (Status, &'static str)> {
    if let Some(s) = declared
        && s.internal_grpc_only
        && !internal_peer_allowed(peer)
    {
        return Err((
            method_security_policy_denied(
                deny_reason::INTERNAL_ONLY,
                "method is restricted to internal callers (loopback peer or verified mTLS client identity required)",
            ),
            deny_reason::INTERNAL_ONLY,
        ));
    }
    Ok(())
}

/// The identity a public-bootstrap request is rate-limited under: forwarded
/// headers count only when a trusted gateway is declared
/// ([`trust_proxy_ip_headers`]); otherwise the transport peer IP. Requests with
/// no resolvable peer (non-transport invocations) share one closed bucket
/// rather than each minting their own.
fn rate_limit_caller_id(
    headers: &http::HeaderMap,
    peer: &TransportPeer,
    trust_proxy_headers: bool,
) -> String {
    if trust_proxy_headers
        && let Some(forwarded) = non_empty_header(headers, "x-forwarded-for")
            .and_then(|value| value.split(',').next())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .or_else(|| non_empty_header(headers, "x-real-ip"))
    {
        return forwarded.to_string();
    }
    peer.remote_addr
        .map(|addr| addr.ip().to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

fn public_rate_limit_key(
    path: &str,
    headers: &http::HeaderMap,
    peer: &TransportPeer,
    policy_ref: Option<&str>,
) -> (String, String, u64) {
    let caller = rate_limit_caller_id(headers, peer, trust_proxy_ip_headers());
    let minute = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs() / 60)
        .unwrap_or_default();
    // C24: fold the named policy into the bucket identity so distinct policies
    // get distinct buckets instead of all sharing one hardcoded bucket.
    let bucket_path = match policy_ref.map(str::trim).filter(|r| !r.is_empty()) {
        Some(reference) => format!("{reference}|{path}"),
        None => path.to_string(),
    };
    (bucket_path, caller, minute)
}

fn public_bootstrap_retry_after_ms() -> i64 {
    let seconds = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default();
    let seconds_until_next_window = 60 - (seconds % 60);
    (seconds_until_next_window.max(1) as i64) * 1_000
}

fn check_public_bootstrap_rate_limit(
    path: &str,
    headers: &http::HeaderMap,
    peer: &TransportPeer,
    policy_ref: Option<&str>,
) -> Result<(), (Status, &'static str)> {
    static BUCKETS: OnceLock<Mutex<HashMap<(String, String, u64), u32>>> = OnceLock::new();
    let key = public_rate_limit_key(path, headers, peer, policy_ref);
    let buckets = BUCKETS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut guard = buckets.lock().map_err(|_| {
        (
            crate::runtime::executor_utils::quota_status(
                "method_security",
                "public bootstrap rate limiter",
                1_000,
                "public bootstrap rate limiter unavailable",
            ),
            deny_reason::PUBLIC_RATE_LIMIT,
        )
    })?;
    let current_minute = key.2;
    guard.retain(|(_, _, minute), _| *minute + 2 >= current_minute);
    let count = guard.entry(key).or_insert(0);
    *count = count.saturating_add(1);
    if *count > public_bootstrap_rate_limit_for(policy_ref) {
        return Err((
            crate::runtime::executor_utils::quota_status(
                "method_security",
                "public_bootstrap_rate_limit",
                public_bootstrap_retry_after_ms(),
                "public bootstrap rate limit exceeded",
            ),
            deny_reason::PUBLIC_RATE_LIMIT,
        ));
    }
    Ok(())
}

fn request_context_required_status() -> Status {
    crate::runtime::executor_utils::invalid_argument_fields(
        "request context required: send x-correlation-id, x-request-id, or traceparent",
        [(
            "request_context",
            "must include x-correlation-id, x-request-id, or traceparent",
        )],
    )
}

/// The verified, claim-derived identity for the current request, captured at the
/// method-security gate AFTER the bearer token validated. Scoped into a task-local
/// ([`current_claim_context`]) so post-decode handlers can compare a request
/// message's BODY tenant/project against the VALIDATED claim — the tower layer
/// only sees `http::Request` headers, so body-vs-claim consistency (D1) cannot be
/// enforced here; it must be enforced by the handler against this captured claim.
///
/// Principle (zero-trust identity): metadata headers may only *narrow/correlate*;
/// the authoritative identity (`subject`/`tenant`/`project`/`scopes`/`roles`)
/// comes from the validated claim. `x-user-id` must NOT override `sub`; a body
/// `tenant_id` must NOT override the claim tenant.
#[derive(Clone, Default, Debug)]
pub struct VerifiedClaimContext {
    pub subject: String,
    pub service_identity: String,
    pub tenant_id: String,
    pub project_id: String,
    pub scopes: Vec<String>,
    pub roles: Vec<String>,
    pub credential_type: i32,
    pub credential_id: String,
    pub auth_method: String,
    /// True only for public bootstrap RPCs (no validated credential): handlers
    /// must NOT treat an empty context as a cross-tenant admin.
    pub authenticated: bool,
}

impl VerifiedClaimContext {
    fn from_principal(principal: &crate::runtime::credential_layer::VerifiedPrincipal) -> Self {
        Self {
            subject: principal.subject.clone(),
            service_identity: principal.service_identity.clone(),
            tenant_id: principal.tenant_id.clone(),
            project_id: principal.project_id.clone(),
            scopes: principal.scopes.clone(),
            roles: principal.roles.clone(),
            credential_type: principal.credential_type,
            credential_id: principal.credential_id.clone(),
            auth_method: principal.auth_method.clone(),
            authenticated: true,
        }
    }

    /// Whether this caller holds a genuine cross-tenant / platform-admin identity,
    /// the only identity permitted to act outside its own claim tenant. Mirrors the
    /// role set used by [`super::apikey`] and the native RLS `app.platform_admin`
    /// escape hatch so the trait-level and row-level gates recognize the SAME
    /// set. Broad control-plane scopes count only on an unbound operator claim;
    /// they never erase a concrete tenant/project boundary. An unauthenticated
    /// (public-bootstrap) context is never an admin (fail-closed).
    pub fn is_cross_tenant_admin(&self) -> bool {
        self.authenticated
            && has_cross_tenant_platform_authority(
                &self.tenant_id,
                &self.project_id,
                &self.scopes,
                &self.roles,
            )
    }

    /// Whether this caller carries one of `required` action scopes (or any broad
    /// admin scope). Used by the per-action authz guard (D2) so an admin-mutation
    /// handler can require a *concrete* scope beyond the coarse `udb:admin` gate.
    fn has_any_scope(&self, required: &[&str]) -> bool {
        self.scopes.iter().any(|scope| {
            let scope = scope.trim();
            ADMIN_SCOPES.contains(&scope) || required.contains(&scope)
        })
    }
}

tokio::task_local! {
    /// The validated claim context for the current request, set by the
    /// method-security gate AFTER the bearer validated. Empty/`authenticated=false`
    /// for public RPCs or outside any scope. Read by native handlers to enforce
    /// body-tenant-vs-claim (D1) and per-action authz (D2) against the VERIFIED
    /// identity — never against request-supplied metadata or body fields.
    static CURRENT_CLAIM_CONTEXT: VerifiedClaimContext;
}

/// The validated claim context for the current request, or a default
/// (`authenticated=false`) when none is in scope (public RPC, or called outside a
/// request). Handlers compare BODY tenant/project against THIS, never against
/// request-supplied values (D1/D2).
pub fn current_claim_context() -> VerifiedClaimContext {
    CURRENT_CLAIM_CONTEXT
        .try_with(|ctx| ctx.clone())
        .unwrap_or_default()
}

/// Whether a claim context is installed at all for the current task. The
/// method-security gate ALWAYS installs one for every transport request — an
/// authenticated context for bearer RPCs, an `authenticated=false` default for
/// public bootstrap RPCs. So "absent" means the call did NOT arrive through the
/// authenticated control-plane transport (an in-process / test / trusted internal
/// caller). The D1/D2 handler guards skip enforcement only in that absent case, so
/// they NEVER weaken a real over-the-wire request (which always has a context) yet
/// keep direct in-process handler invocation working.
pub fn claim_context_present() -> bool {
    CURRENT_CLAIM_CONTEXT.try_with(|_| ()).is_ok()
}

/// Run `fut` with `ctx` installed as the current validated claim context.
async fn scope_claim_context<F>(ctx: VerifiedClaimContext, fut: F) -> F::Output
where
    F: Future,
{
    CURRENT_CLAIM_CONTEXT.scope(ctx, fut).await
}

/// Test-only: run async `fut` with `ctx` installed as the current claim context,
/// exactly as the tower layer does for a real request. Lets handler tests in other
/// modules (lifecycle/core) drive the D1/D2/D3 guards under a chosen identity.
#[cfg(test)]
#[doc(hidden)]
pub async fn scope_claim_context_for_test<F>(ctx: VerifiedClaimContext, fut: F) -> F::Output
where
    F: Future,
{
    scope_claim_context(ctx, fut).await
}

/// Test-only constructor for a validated claim context.
#[cfg(test)]
#[doc(hidden)]
pub fn test_claim_context(
    subject: &str,
    tenant_id: &str,
    project_id: &str,
    scopes: &[&str],
    roles: &[&str],
) -> VerifiedClaimContext {
    VerifiedClaimContext {
        subject: subject.to_string(),
        service_identity: String::new(),
        tenant_id: tenant_id.to_string(),
        project_id: project_id.to_string(),
        scopes: scopes.iter().map(|s| s.to_string()).collect(),
        roles: roles.iter().map(|r| r.to_string()).collect(),
        credential_type: 1,
        credential_id: String::new(),
        auth_method: "test".to_string(),
        authenticated: true,
    }
}

/// D1 — post-decode body-tenant/project guard. The tower layer only sees headers,
/// so a decoded protobuf BODY `tenant_id`/`project_id` is never compared to the
/// claim at the transport gate. Handlers call this AFTER decoding so a tenant-A
/// admin token cannot act on tenant B by setting a body `tenant_id=B`.
///
/// Rules (fail-closed): a non-empty body tenant/project must equal the VALIDATED
/// claim tenant/project, UNLESS the caller holds a genuine cross-tenant admin
/// identity ([`VerifiedClaimContext::is_cross_tenant_admin`]). An empty body
/// field inherits the claim scope (no override) and is allowed. A mismatch is
/// DENIED and audited via tracing (auditable: it carries the subject + both
/// tenants).
pub fn enforce_body_tenant_matches_claim(
    ctx: &VerifiedClaimContext,
    body_tenant: &str,
    body_project: &str,
) -> Result<(), Status> {
    // No claim context installed → not an over-the-wire request (in-process /
    // trusted / test caller). Real transport requests always carry a context, so
    // skipping here never weakens them. See [`claim_context_present`].
    if !claim_context_present() {
        return Ok(());
    }
    if ctx.is_cross_tenant_admin() {
        return Ok(());
    }
    let body_tenant = body_tenant.trim();
    let claim_tenant = ctx.tenant_id.trim();
    // A non-platform transport identity must be anchored to a verified tenant.
    // Supplying the missing tenant in the request body cannot create authority;
    // this also closes the project-only broad-scope shape (project set, tenant
    // empty) before a handler can inherit or persist a caller-selected tenant.
    if claim_tenant.is_empty() {
        tracing::warn!(
            target: "udb.audit.authz",
            subject = %ctx.subject,
            service_identity = %ctx.service_identity,
            credential_type = ctx.credential_type,
            credential_id = %ctx.credential_id,
            body_tenant = %body_tenant,
            "DENY: tenant-scoped operation requires a tenant-bound bearer or cross-tenant admin"
        );
        return Err(method_security_policy_denied(
            deny_reason::TENANT_REQUIRED,
            "operation requires a tenant-scoped bearer token or a cross-tenant admin role",
        ));
    }
    if !body_tenant.is_empty() && !claim_tenant.is_empty() && body_tenant != claim_tenant {
        tracing::warn!(
            target: "udb.audit.authz",
            subject = %ctx.subject,
            service_identity = %ctx.service_identity,
            credential_type = ctx.credential_type,
            credential_id = %ctx.credential_id,
            claim_tenant = %claim_tenant,
            body_tenant = %body_tenant,
            "DENY: request body tenant does not match the validated bearer tenant"
        );
        return Err(method_security_policy_denied(
            deny_reason::TENANT_MISMATCH,
            "request tenant must match the bearer token tenant",
        ));
    }
    let body_project = body_project.trim();
    let claim_project = ctx.project_id.trim();
    if !body_project.is_empty() && !claim_project.is_empty() && body_project != claim_project {
        tracing::warn!(
            target: "udb.audit.authz",
            subject = %ctx.subject,
            service_identity = %ctx.service_identity,
            credential_type = ctx.credential_type,
            credential_id = %ctx.credential_id,
            claim_project = %claim_project,
            body_project = %body_project,
            "DENY: request body project does not match the validated bearer project"
        );
        return Err(method_security_policy_denied(
            deny_reason::PROJECT_MISMATCH,
            "request project must match the bearer token project",
        ));
    }
    Ok(())
}

/// C23 — post-decode body-owner guard, the ownership analogue of
/// [`enforce_body_tenant_matches_claim`]. When an RPC declares an
/// `endpoint_security.owner_field`, a handler extracts that field from the
/// decoded body and calls this so that — WITHIN a tenant — a caller cannot act on
/// another principal's owned resource by naming a foreign owner. Fail-closed: a
/// non-empty body owner must equal the validated principal subject, unless the
/// caller is a genuine cross-tenant admin. An empty body owner inherits the
/// principal (no override). Skipped on the in-process/loopback path (no claim
/// context installed), matching the tenant guard.
pub fn enforce_body_owner_matches_claim(
    ctx: &VerifiedClaimContext,
    body_owner: &str,
) -> Result<(), Status> {
    if !claim_context_present() {
        return Ok(());
    }
    if ctx.is_cross_tenant_admin() {
        return Ok(());
    }
    let body_owner = body_owner.trim();
    let subject = ctx.subject.trim();
    if !body_owner.is_empty() && !subject.is_empty() && body_owner != subject {
        tracing::warn!(
            target: "udb.audit.authz",
            subject = %ctx.subject,
            service_identity = %ctx.service_identity,
            credential_type = ctx.credential_type,
            credential_id = %ctx.credential_id,
            body_owner = %body_owner,
            "DENY: request body owner does not match the authenticated principal"
        );
        return Err(method_security_policy_denied(
            deny_reason::OWNER,
            "request owner must match the authenticated principal",
        ));
    }
    Ok(())
}

/// 01.4.1.1 — the write-path analogue of [`enforce_body_tenant_matches_claim`].
/// It applies the SAME mismatch-reject + both-empty fail-closed checks, then
/// RESOLVES the effective `(tenant, project)` to STORE:
/// - a non-empty body returns the (validated-equal) body values;
/// - an empty body with a tenant-bound claim returns the claim tenant/project, so
///   an omitted body is persisted as the claim tenant — NOT `""` (which would be
///   invisible to claim-bound reads and force clients to copy the claim tenant
///   into every write body);
/// - a genuine cross-tenant admin returns the body values verbatim (may target any
///   tenant);
/// - with no claim context installed (in-process/loopback path) the body values
///   pass through unchanged.
pub fn resolve_body_tenant_scope(
    ctx: &VerifiedClaimContext,
    body_tenant: &str,
    body_project: &str,
) -> Result<(String, String), Status> {
    // No claim context installed → not an over-the-wire request. Preserve the body.
    if !claim_context_present() {
        return Ok((body_tenant.to_string(), body_project.to_string()));
    }
    // Reuse the validate-only guard: rejects a body↔claim mismatch and the
    // tenant-less (both-empty, non-admin) case before we resolve anything.
    enforce_body_tenant_matches_claim(ctx, body_tenant, body_project)?;
    // A cross-tenant admin may legitimately target a foreign/unscoped tenant: keep
    // whatever the body asked for verbatim.
    if ctx.is_cross_tenant_admin() {
        return Ok((body_tenant.to_string(), body_project.to_string()));
    }
    let body_tenant = body_tenant.trim();
    let body_project = body_project.trim();
    let tenant = if body_tenant.is_empty() {
        ctx.tenant_id.trim().to_string()
    } else {
        body_tenant.to_string()
    };
    let project = if body_project.is_empty() {
        ctx.project_id.trim().to_string()
    } else {
        body_project.to_string()
    };
    Ok((tenant, project))
}

/// D2 — per-action authz guard for admin-mutation handlers. The coarse `udb:admin`
/// scope from the transport gate authorizes ANY non-public RPC; this requires a
/// *concrete* action scope on top so the handler records a real per-action
/// authorization decision (e.g. `authn.user.create`). On a negative decision it
/// DENIES and audits via tracing. On success it returns a fresh `decision_id` the
/// handler can thread into its audit/outbox event.
///
/// `action` is the logical action (`authn.user.create`); `required_scopes` are the
/// concrete scopes that authorize it (the broad admin scopes always satisfy it, so
/// existing legitimate-admin callers keep working). A caller that authenticated
/// but carries neither a broad admin scope nor an action scope is denied — closing
/// the gap where the coarse gate was the ONLY check.
pub fn authorize_action(
    ctx: &VerifiedClaimContext,
    action: &str,
    required_scopes: &[&str],
) -> Result<String, Status> {
    // No claim context installed → not an over-the-wire request (in-process /
    // trusted / test caller). Real transport requests always carry a context.
    if !claim_context_present() {
        return Ok(uuid::Uuid::new_v4().to_string());
    }
    if !ctx.authenticated {
        return Err(Status::unauthenticated(
            "action requires an authenticated principal",
        ));
    }
    if ctx.has_any_scope(required_scopes) {
        return Ok(uuid::Uuid::new_v4().to_string());
    }
    tracing::warn!(
        target: "udb.audit.authz",
        subject = %ctx.subject,
        tenant = %ctx.tenant_id,
        action = %action,
        "DENY: principal lacks the action-specific scope for this admin mutation"
    );
    Err(method_security_policy_denied(
        deny_reason::SCOPE,
        format!(
            "action '{action}' requires one of the scopes {required_scopes:?} (or a control-plane admin scope)"
        ),
    ))
}

/// The `policy_ref` declared on `path`'s `endpoint_security`, if any. Lets a
/// per-action native authz decision (D2-full) know which named policy set to
/// evaluate the action against.
pub fn method_policy_ref(path: &str) -> Option<String> {
    method_security(path).and_then(|s| s.policy_ref.clone())
}

/// The resource string to authorize an action for `path` against: the
/// descriptor's `endpoint_security.decision_resource` when set, else the
/// synthesized `native.rpc:{service}/{method}` so the native authz DECISION
/// ENGINE always has a concrete resource to evaluate (D2-full, step 3).
///
/// `path` is the gRPC path (`/<package>.<Service>/<Method>`); the synthetic form
/// drops the leading `/` and prefixes `native.rpc:` so it cannot collide with a
/// data-plane message/table resource.
pub fn decision_resource_for(path: &str) -> String {
    if let Some(resource) = method_security(path).and_then(|s| s.decision_resource.clone()) {
        return resource;
    }
    format!("native.rpc:{}", path.trim_start_matches('/'))
}

impl VerifiedClaimContext {
    /// Project the validated claim identity into an authz [`Principal`] so the
    /// native authz DECISION ENGINE can evaluate a per-action policy decision
    /// against the SAME verified identity the transport gate established (never
    /// request-supplied metadata). Used by the admin-mutation handlers (D2-full).
    pub fn to_principal(&self) -> crate::runtime::authz::Principal {
        crate::runtime::authz::Principal {
            principal_id: self.subject.clone(),
            subject: self.subject.clone(),
            user_id: self.subject.clone(),
            service_identity: self.service_identity.clone(),
            tenant_id: self.tenant_id.clone(),
            project_id: self.project_id.clone(),
            scopes: self.scopes.clone(),
            roles: self.roles.clone(),
            provider_id: String::new(),
            auth_method: String::new(),
        }
    }
}

// NOTE (R3.1): a single `bind_claim_scope`/`BoundClaimScope` projection was
// considered as the central authority binder, but every served call site that
// builds an authz `Principal` from the verified claim follows a deliberately
// different contract than a one-shot "claim → principal + request context"
// bundle would impose:
//   * authz/mod.rs (Authorize / CheckAccess / MintNativeAccess) and
//     governance.rs VALIDATE the body tenant/project against the claim with
//     `enforce_body_tenant_matches_claim`, then OVERRIDE individual principal
//     fields from the claim ONLY for non-admins — a genuine cross-tenant admin
//     deliberately KEEPS the body-derived principal to answer "can X do Y?".
//     A claim-only projection would clobber that admin path.
//   * `created_by`/`assigned_by` binders use `stable_uuid_from_subject` +
//     forge-guarding, not a principal projection.
//   * native_helpers (`validate_request_scope`, `native_service_context`,
//     `metadata_tenant_id`) layer x-tenant-id/x-project-id header-consistency
//     checks and a metadata-sourced `RequestContext` that a claim-only bundle
//     does not model.
//   * authn/core.rs CreateUser already calls `resolve_body_tenant_scope`
//     directly and threads `claim_ctx` into `authorize_action` /
//     `decide_action_native`; the bundle's principal/request_context would be
//     unused.
// The reusable primitives (`current_claim_context`, `resolve_body_tenant_scope`,
// `to_principal`, `enforce_body_tenant_matches_claim`) ARE the shared authority
// binders and are already adopted at those sites; a wrapper bundle could not be
// adopted anywhere without changing behavior, so it was removed to clear the
// dead-code warning rather than leave an unused capability.

/// Decide whether a request for `path` carrying `headers` may proceed. On denial
/// returns the `Status` plus a stable `reason` label so the caller can record
/// `udb_method_security_denials_total{reason}` (and `udb_tenant_mismatch_total`
/// for the tenant-mismatch reasons) — Phase 10 telemetry coherence.
/// On success returns the canonical verified principal plus the full validated
/// [`VerifiedClaimContext`] so the caller can scope credential lineage for audit
/// attribution and post-decode body-tenant/per-action authz in handlers. Public
/// bootstrap RPCs return empty/default values because no credential was validated.
fn enforce(
    security: &SecurityConfig,
    path: &str,
    headers: &http::HeaderMap,
    peer: &TransportPeer,
    preresolved: Option<&crate::runtime::credential_layer::PreresolvedCredentials>,
) -> Result<
    (
        crate::runtime::credential_layer::VerifiedPrincipal,
        VerifiedClaimContext,
    ),
    (Status, &'static str),
> {
    if let Some(service_id) =
        crate::runtime::service::native_registry::native_service_for_grpc_path(path)
        && !crate::runtime::service::native_registry::native_service_enabled(&service_id)
    {
        return Err((
            crate::runtime::executor_utils::capability_status(
                "native_service",
                format!("{service_id}/dispatch"),
                "native_service_enabled",
                format!("native service '{service_id}' is disabled"),
            ),
            deny_reason::DISABLED,
        ));
    }
    let declared = method_security(path);

    // Public bootstrap RPCs need no control-plane bearer.
    if matches!(declared, Some(s) if s.mode == AuthMode::Public) {
        check_public_bootstrap_rate_limit(
            path,
            headers,
            peer,
            declared.and_then(|s| s.rate_limit_policy_ref.as_deref()),
        )?;
        if let Some(s) = declared {
            if !s.allowed_credential_types.is_empty()
                && !s
                    .allowed_credential_types
                    .contains(&(CredentialType::BearerJwt as i32))
            {
                return Err((
                    method_security_policy_denied(
                        deny_reason::PUBLIC_CRED,
                        "public method credential contract does not allow bearerless access",
                    ),
                    deny_reason::PUBLIC_CRED,
                ));
            }
        }
        // Public bootstrap: no validated principal to attribute.
        return Ok((
            crate::runtime::credential_layer::VerifiedPrincipal::default(),
            VerifiedClaimContext::default(),
        ));
    }

    // Annotated non-public, or unannotated (fail closed): require a valid bearer.
    let token = bearer_token(headers);
    // UDB-AUTH-008: the credential layer's resolved API-key principal (derived
    // ENTIRELY from the stored key record + owner grant), when present.
    let api_key_outcome = preresolved.and_then(|resolved| resolved.api_key.as_ref());
    let api_key_principal = api_key_outcome
        .and_then(|outcome| outcome.as_ref().ok())
        .and_then(|record| record.as_ref());
    // UDB-DB-READINESS-001: distinguish a durable-store OUTAGE during api-key
    // validation (resolver Err tagged Unavailable) from a clean miss (Ok(None)).
    // The `.ok()` above intentionally treats a miss as "no key"; an OUTAGE must
    // instead surface as a retryable Unavailable, never a missing/invalid key.
    let api_key_store_unavailable = matches!(
        api_key_outcome,
        Some(Err(reason))
            if crate::runtime::executor_utils::status_from_store_string(reason.clone()).code()
                == tonic::Code::Unavailable
    );
    // Whether THIS method opts into scoped API-key workload credentials.
    let method_allows_api_key = matches!(
        declared,
        Some(s) if s.allowed_credential_types.contains(&(CredentialType::ApiKey as i32))
    );
    if let Some(credential) = direct_credential_type(headers) {
        if token.is_some() {
            return Err((
                method_security_policy_denied(
                    deny_reason::CREDENTIAL_TYPE,
                    "request carries multiple credential types; send exactly one credential",
                ),
                deny_reason::CREDENTIAL_TYPE,
            ));
        }
        // UDB-AUTH-008: a scoped service API key is a first-class credential on
        // a method that declares CREDENTIAL_TYPE_API_KEY (the layer already
        // validated it against the stored record + current grant). Only then
        // do we skip the bearer-exchange denial; every other case still fails
        // closed exactly as before.
        let api_key_accepted = credential == CredentialType::ApiKey
            && method_allows_api_key
            && api_key_principal.is_some();
        if !api_key_accepted {
            enforce_direct_credential_type(declared, credential)?;
        }
    }
    // fix_plan §1.4/§3: a REGISTERED mTLS principal — resolved by the async
    // credential layer through the durable certificate-binding → grant chain —
    // authenticates a native method ONLY when the descriptor EXPLICITLY allows
    // CREDENTIAL_TYPE_MTLS. No descriptor opt-in (or no registered binding) →
    // fail closed exactly as before. Declared method scopes still apply; a
    // grant can never carry admin/wildcard scopes (rejected at write time).
    // HIGH-4: an mTLS principal does NOT return early — it synthesizes the
    // same verified-claims shape a bearer produces and FALLS THROUGH every
    // common post-authentication gate below (scope/admin, roles,
    // internal-only, CSRF, request-context, tenant/project consistency), so
    // certificate-authenticated requests face identical policy. Accepted ONLY
    // when the descriptor EXPLICITLY allows CREDENTIAL_TYPE_MTLS; a grant can
    // never carry admin/wildcard scopes (rejected at write time).
    let (claims, verified_principal) = if token.is_none()
        && let Some(principal) =
            preresolved.and_then(|resolved| resolved.certificate_principal.as_ref())
        && matches!(
            declared,
            Some(s) if s
                .allowed_credential_types
                .contains(&(CredentialType::Mtls as i32))
        ) {
        (
            crate::runtime::security::claims_from_verified_principal(principal),
            principal.clone(),
        )
    } else if token.is_none()
        && method_allows_api_key
        && let Some(principal) = api_key_principal
    {
        // UDB-AUTH-008: scoped service API key accepted on this native method.
        // Same fall-through as mTLS — it synthesizes verified claims and faces
        // every common post-authentication gate (scope/admin, roles,
        // internal-only, CSRF, request-context, tenant/project). Its scopes are
        // the stored key's grant-attenuated set, never admin/wildcard.
        (
            crate::runtime::security::claims_from_verified_principal(principal),
            principal.clone(),
        )
    } else {
        // A5: an api-key request on a method that accepts api-keys, whose durable
        // store was UNAVAILABLE (outage — not a bad/missing key), surfaces a
        // retryable Unavailable rather than a misleading missing-bearer
        // Unauthenticated (UDB-DB-READINESS-001).
        if token.is_none() && method_allows_api_key && api_key_store_unavailable {
            return Err((
                crate::runtime::executor_utils::retryable_status(
                    "authn",
                    "api_key_validate",
                    crate::runtime::executor_utils::HTTP_RETRYABLE_BACKOFF_MS,
                    "credential store temporarily unavailable",
                ),
                deny_reason::STORE_UNAVAILABLE,
            ));
        }
        let token = token.ok_or_else(|| {
            (
                Status::unauthenticated(
                    "missing or invalid authorization header (native control-plane bearer required)",
                ),
                deny_reason::MISSING_BEARER,
            )
        })?;
        let (claims, principal) = match preresolved.and_then(|resolved| resolved.bearer.as_ref()) {
            Some(Ok(principal)) => (
                crate::runtime::security::claims_from_verified_principal(principal),
                principal.clone(),
            ),
            Some(Err(reason)) => {
                // A transient outage while validating the service bearer's grant
                // is a retryable dependency failure, not a bad credential. The
                // resolver tags a store outage as Unavailable; a genuine
                // invalid/ungranted bearer stays Unauthenticated (fail closed).
                // See UDB-DB-READINESS-001.
                let status =
                    crate::runtime::executor_utils::status_from_store_string(reason.clone());
                if status.code() == tonic::Code::Unavailable {
                    return Err((status, deny_reason::STORE_UNAVAILABLE));
                }
                return Err((
                    Status::unauthenticated("invalid bearer token"),
                    deny_reason::INVALID_BEARER,
                ));
            }
            None => {
                let claims = validate_bearer_token(security, token)
                    .map_err(|e| (Status::unauthenticated(e), deny_reason::INVALID_BEARER))?;
                let principal =
                    crate::runtime::credential_layer::VerifiedPrincipal::from_verified_bearer_claims(
                        &claims,
                    );
                (claims, principal)
            }
        };
        enforce_bearer_credential_type(declared, &claims)?;
        (claims, principal)
    };
    // A verified client certificate remains transport identity when a bearer
    // is also present. Both credentials must resolve to the exact same service,
    // tenant, and project; otherwise this is a credential splice. This runs on
    // the native listener as well as DataBroker and consumes the same principal
    // extension, so neither listener can silently ignore a bound certificate.
    if token.is_some()
        && let Some(binding) =
            preresolved.and_then(|resolved| resolved.certificate_principal.as_ref())
    {
        crate::runtime::security::enforce_certificate_credential_composition(
            binding,
            claims.service_identity.as_deref().unwrap_or_default(),
            claims.tenant_id.as_deref().unwrap_or_default(),
            claims.project_id.as_deref().unwrap_or_default(),
        )
        .map_err(|status| (status, deny_reason::CREDENTIAL_STATE))?;
    }
    let scopes = claims.resolved_scopes();
    let has_admin = scopes
        .iter()
        .any(|scope| ADMIN_SCOPES.contains(&scope.as_str()));

    // Baseline preserved: the admin/auth-admin scope authorizes any non-public
    // RPC. A method may additionally declare narrower scopes; a least-privilege
    // service token carrying one of those is also accepted.
    let method_scopes = declared.map(|s| s.scopes.as_slice()).unwrap_or(&[]);
    let has_declared_scope = scopes
        .iter()
        .any(|scope| method_scopes.iter().any(|declared| declared == scope));
    if !has_admin && !has_declared_scope {
        return Err((
            method_security_policy_denied(
                deny_reason::SCOPE,
                "scope udb:admin or udb:auth:admin is required",
            ),
            deny_reason::SCOPE,
        ));
    }

    // Defense in depth: if the method declares roles, a non-admin caller must
    // carry one of them.
    if let Some(s) = declared
        && !s.roles.is_empty()
        && !has_admin
    {
        let roles = claims.roles.clone().unwrap_or_default();
        if !roles.iter().any(|role| s.roles.iter().any(|d| d == role)) {
            return Err((
                method_security_policy_denied(
                    deny_reason::ROLE,
                    "required role missing for this control-plane method",
                ),
                deny_reason::ROLE,
            ));
        }
    }

    // C21: assurance-level (AAL / MFA step-up) gate. An RPC declaring
    // `required_assurance_level=N` demands a bearer minted at that assurance
    // (the `acr` claim: `aal2` ⇒ 2, `aal1`/absent ⇒ 1). Enforced independently of
    // scope/role — an admin scope does NOT waive a step-up requirement. When no
    // RPC declares a level (the current descriptor set) this gate never fires.
    if let Some(s) = declared
        && s.required_assurance_level > 1
    {
        let caller_aal = claims.acr.as_deref().map(aal_level).unwrap_or(1);
        if caller_aal < s.required_assurance_level {
            return Err((
                method_security_policy_denied(
                    deny_reason::ASSURANCE,
                    "method requires a higher authentication assurance level (MFA step-up)",
                ),
                deny_reason::ASSURANCE,
            ));
        }
    }

    if let Some(s) = declared {
        enforce_internal_grpc_only(Some(s), peer)?;
        if s.csrf_required
            && non_empty_header(headers, "x-csrf-token").is_none()
            && !header_truthy(headers, "x-udb-csrf-validated")
        {
            return Err((
                method_security_policy_denied(
                    deny_reason::CSRF,
                    "csrf token or validated csrf marker is required",
                ),
                deny_reason::CSRF,
            ));
        }
        if s.request_context_required
            && non_empty_header(headers, "x-correlation-id").is_none()
            && non_empty_header(headers, "traceparent").is_none()
            && non_empty_header(headers, "x-request-id").is_none()
        {
            return Err((
                request_context_required_status(),
                deny_reason::REQUEST_CONTEXT,
            ));
        }
        if s.tenant_required
            && claims
                .tenant_id
                .as_deref()
                .unwrap_or_default()
                .trim()
                .is_empty()
        {
            return Err((
                method_security_policy_denied(
                    deny_reason::TENANT_REQUIRED,
                    "method requires a tenant-scoped bearer token",
                ),
                deny_reason::TENANT_REQUIRED,
            ));
        }
        if !s.tenant_field.trim().is_empty()
            && non_empty_header(headers, "x-tenant-id").is_none()
            && claims
                .tenant_id
                .as_deref()
                .unwrap_or_default()
                .trim()
                .is_empty()
        {
            return Err((
                method_security_policy_denied(
                    deny_reason::TENANT_REQUIRED,
                    "method requires a tenant-scoped request context",
                ),
                deny_reason::TENANT_REQUIRED,
            ));
        }
    }

    // Tenant/project consistency: explicit metadata must match bearer claims.
    if let Some(header_tenant) = non_empty_header(headers, "x-tenant-id")
        && claims.tenant_id.as_deref().unwrap_or_default() != header_tenant
    {
        return Err((
            method_security_policy_denied(
                deny_reason::TENANT_MISMATCH,
                "x-tenant-id must match the bearer token tenant",
            ),
            deny_reason::TENANT_MISMATCH,
        ));
    }
    if let Some(s) = declared
        && (s.tenant_required || !s.tenant_field.trim().is_empty())
        && let Some(header_tenant) = non_empty_header(headers, "x-tenant-id")
        && claims.tenant_id.as_deref().unwrap_or_default() != header_tenant
    {
        return Err((
            method_security_policy_denied(
                deny_reason::TENANT_MISMATCH,
                "request tenant metadata must match the bearer token tenant",
            ),
            deny_reason::TENANT_MISMATCH,
        ));
    }
    if let Some(header_project) = project_header(headers) {
        let claim_project = claims.project_id.as_deref().unwrap_or_default();
        if !claim_project.is_empty() && claim_project != header_project {
            return Err((
                method_security_policy_denied(
                    deny_reason::PROJECT_MISMATCH,
                    "project metadata must match the bearer token project",
                ),
                deny_reason::PROJECT_MISMATCH,
            ));
        }
    }
    if let Some(s) = declared
        && !s.project_field.trim().is_empty()
        && project_header(headers).is_none()
        && claims
            .project_id
            .as_deref()
            .unwrap_or_default()
            .trim()
            .is_empty()
    {
        return Err((
            method_security_policy_denied(
                deny_reason::PROJECT_REQUIRED,
                "method requires a project-scoped request context",
            ),
            deny_reason::PROJECT_REQUIRED,
        ));
    }

    let claim_ctx = VerifiedClaimContext::from_principal(&verified_principal);
    // Fail-closed tenant-status gate: a caller whose OWN claim tenant this node has
    // observed SUSPENDED/INACTIVE is rejected here — revoking its live bearer tokens
    // immediately rather than at their TTL. Public/in-process callers have an empty
    // claim tenant, on which the gate no-ops. Keying on the caller's own tenant means
    // a cross-tenant/platform admin can still reach UpdateTenant to reactivate.
    if claim_ctx.authenticated {
        crate::runtime::service::tenant_service::tenant_status_gate(&claim_ctx.tenant_id)
            .map_err(|status| (status, deny_reason::TENANT_SUSPENDED))?;
    }
    Ok((verified_principal, claim_ctx))
}

/// Mirror of `udb.core.common.v1.CredentialType`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CredentialType {
    BearerJwt = 1,
    Session = 2,
    ApiKey = 3,
    ServiceAccount = 4,
    /// fix_plan §1.4: a request authenticated by a REGISTERED peer certificate
    /// (durable binding → grant chain) — `CREDENTIAL_TYPE_MTLS = 5`.
    Mtls = 5,
}

impl CredentialType {
    fn label(self) -> &'static str {
        match self {
            CredentialType::BearerJwt => "bearer JWT",
            CredentialType::Session => "session",
            CredentialType::ApiKey => "API key",
            CredentialType::ServiceAccount => "service account",
            CredentialType::Mtls => "mTLS certificate",
        }
    }
}

/// Every native control-plane RPC path (`/<package>.<Service>/<Method>`) for
/// `udb.core.*` *service* packages, whether or not it declares
/// `endpoint_security`. Used by the conformance gate to assert full annotation
/// coverage (the registry only contains annotated RPCs, so any path here that is
/// absent from the registry is an unannotated RPC).
///
/// Test-only: its sole consumer is the `every_native_control_plane_rpc_is_annotated`
/// conformance gate below. At runtime the decoded registry
/// ([`method_security_registry`]) is the source of truth and the `enforce` path
/// fails closed on any unannotated path, so the full-coverage enumeration is needed
/// only to ASSERT annotation completeness in the test, not on the live request path.
#[cfg(test)]
pub fn all_native_service_rpc_paths() -> Vec<String> {
    let bytes = crate::runtime::native_catalog::embedded_file_descriptor_set();
    let mut out = Vec::new();
    let Ok(set) = <prost_types::FileDescriptorSet as prost::Message>::decode(bytes) else {
        return out;
    };
    for file in &set.file {
        let Some(package) = file.package.as_deref() else {
            continue;
        };
        // Native control-plane services live under `udb.core.<domain>.services.<v>`.
        if !(package.starts_with("udb.core.") && package.contains(".services.")) {
            continue;
        }
        for svc in &file.service {
            let Some(svc_name) = svc.name.as_deref() else {
                continue;
            };
            for method in &svc.method {
                if let Some(method_name) = method.name.as_deref() {
                    out.push(format!("/{package}.{svc_name}/{method_name}"));
                }
            }
        }
    }
    out
}

/// Tower layer that enforces [`method_security`] on the native control plane.
#[derive(Clone)]
pub struct MethodSecurityLayer {
    security: SecurityConfig,
    /// Optional Phase-10 metrics handle. When set, the layer records
    /// `udb_method_security_denials_total{reason}` and `udb_tenant_mismatch_total`
    /// on every rejection. Wired by the parent in `service/mod.rs` via
    /// [`MethodSecurityLayer::with_metrics`]; `None` keeps the slim/no-metrics
    /// path working.
    metrics: Option<Arc<dyn MetricsRecorder>>,
}

impl MethodSecurityLayer {
    pub fn new() -> Self {
        Self {
            security: SecurityConfig::current(),
            metrics: None,
        }
    }

    /// Attach the shared metrics recorder so per-RPC denials and tenant
    /// mismatches are counted (Phase 10). The parent passes the same
    /// `Arc<dyn MetricsRecorder>` it wires into the rest of the runtime.
    pub fn with_metrics(mut self, metrics: Arc<dyn MetricsRecorder>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Wrap a tonic service so every request is gated by its proto-declared
    /// method security. Kept as an inherent method (rather than the `tower::Layer`
    /// trait) so the wiring reads `msec.wrap(XServer::new(svc))`.
    pub fn wrap<S>(&self, inner: S) -> MethodSecurityService<S> {
        MethodSecurityService {
            inner,
            security: self.security.clone(),
            metrics: self.metrics.clone(),
        }
    }
}

impl Default for MethodSecurityLayer {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone)]
pub struct MethodSecurityService<S> {
    inner: S,
    security: SecurityConfig,
    metrics: Option<Arc<dyn MetricsRecorder>>,
}

// Preserve the wrapped service's gRPC name so tonic's router still dispatches it.
impl<S> tonic::server::NamedService for MethodSecurityService<S>
where
    S: tonic::server::NamedService,
{
    const NAME: &'static str = S::NAME;
}

impl<S, ReqBody> Service<http::Request<ReqBody>> for MethodSecurityService<S>
where
    S: Service<http::Request<ReqBody>, Response = http::Response<BoxBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    ReqBody: Send + 'static,
{
    type Response = http::Response<BoxBody>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<http::Response<BoxBody>, S::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<ReqBody>) -> Self::Future {
        let path = req.uri().path().to_string();
        // Transport facts (peer socket addr / mTLS client identity) ride in the
        // connect-info extensions tonic installs on every served request; the
        // rate limiter and the internal_grpc_only gate key off them, never off
        // forgeable headers.
        let peer = transport_peer(req.extensions());
        let pre_cred = req
            .extensions()
            .get::<crate::runtime::credential_layer::PreresolvedCredentials>();
        let certificate_resolution_unavailable = pre_cred
            .map(|pre| pre.certificate_resolution_unavailable)
            .unwrap_or(false);
        let certificate_resolution_failed = pre_cred
            .map(|pre| {
                pre.certificate_resolution_failed
                    || (pre.certificate_present && pre.certificate_principal.is_none())
            })
            .unwrap_or(false);
        let is_public_method =
            matches!(method_security(&path), Some(s) if s.mode == AuthMode::Public);
        if certificate_resolution_unavailable && !is_public_method {
            // A transient DB outage while resolving the certificate binding is a
            // retryable dependency failure, not a bad certificate. See
            // UDB-DB-READINESS-001.
            if let Some(metrics) = self.metrics.as_ref() {
                metrics.inc_method_security_denial(deny_reason::STORE_UNAVAILABLE);
            }
            let status = crate::runtime::executor_utils::retryable_status(
                "authn",
                "certificate_resolution",
                crate::runtime::executor_utils::HTTP_RETRYABLE_BACKOFF_MS,
                "credential store temporarily unavailable",
            );
            return Box::pin(async move { Ok(status.into_http()) });
        }
        if certificate_resolution_failed && !is_public_method {
            if let Some(metrics) = self.metrics.as_ref() {
                metrics.inc_method_security_denial(deny_reason::CREDENTIAL_STATE);
            }
            let status = Status::unauthenticated(
                "client certificate binding could not be verified; request denied",
            );
            return Box::pin(async move { Ok(status.into_http()) });
        }
        let preresolved = req
            .extensions()
            .get::<crate::runtime::credential_layer::PreresolvedCredentials>();
        let audit_principal = preresolved
            .and_then(|resolved| {
                if bearer_token(req.headers()).is_some() {
                    resolved
                        .bearer
                        .as_ref()
                        .and_then(|result| result.as_ref().ok())
                } else {
                    resolved.certificate_principal.as_ref()
                }
            })
            .cloned();
        match enforce(&self.security, &path, req.headers(), &peer, preresolved) {
            Ok((verified, claim_ctx)) => {
                // Phase 10: scope the authenticated principal + the gate
                // authorization decision so native handlers' outbox events attribute
                // the real `actor`/`auth_method`/`decision_id`/`policy_revision`
                // without per-handler threading. The method-security gate IS an
                // authorization decision (it enforced the endpoint-security policy);
                // record it with a fresh decision id + the contract revision whose
                // rules it applied. Public/unauthenticated RPCs scope an empty
                // principal (no validated decision to attribute).
                let (decision_id, policy_revision) = if verified.subject.is_empty() {
                    (String::new(), String::new())
                } else {
                    (
                        uuid::Uuid::new_v4().to_string(),
                        crate::runtime::descriptor_diff::NATIVE_CONTRACT_VERSION.to_string(),
                    )
                };
                let principal = crate::runtime::otel::RequestPrincipal {
                    subject: verified.subject,
                    service_identity: verified.service_identity,
                    credential_type: verified.credential_type,
                    credential_id: verified.credential_id,
                    auth_method: verified.auth_method,
                    decision_id,
                    policy_revision,
                };
                if !principal.subject.is_empty() {
                    tracing::info!(
                        target: "udb.audit.authz",
                        method = %path,
                        outcome = "allow",
                        subject = %principal.subject,
                        service_identity = %principal.service_identity,
                        credential_type = principal.credential_type,
                        credential_id = %principal.credential_id,
                        auth_method = %principal.auth_method,
                        decision_id = %principal.decision_id,
                        policy_revision = %principal.policy_revision,
                        "native method authorization allowed"
                    );
                }
                // Scope BOTH the audit principal (otel) AND the full validated
                // claim context (this module) for the duration of the handler, so
                // post-decode handlers can enforce body-tenant-vs-claim (D1) and
                // per-action authz (D2) against the VERIFIED identity. Nested so the
                // claim context is live wherever the principal is.
                let fut = scope_claim_context(claim_ctx, self.inner.call(req));
                Box::pin(crate::runtime::otel::scope_principal(principal, fut))
            }
            Err((status, reason)) => {
                // Phase 10: count the denial by reason, and the tenant-mismatch
                // sub-case on its own dedicated counter.
                if let Some(metrics) = self.metrics.as_ref() {
                    metrics.inc_method_security_denial(reason);
                    if reason == deny_reason::TENANT_MISMATCH {
                        metrics.inc_tenant_mismatch();
                    }
                }
                let audit_principal = audit_principal.unwrap_or_default();
                tracing::warn!(
                    target: "udb.audit.authz",
                    method = %path,
                    outcome = "deny",
                    reason,
                    subject = %audit_principal.subject,
                    service_identity = %audit_principal.service_identity,
                    credential_type = audit_principal.credential_type,
                    credential_id = %audit_principal.credential_id,
                    auth_method = %audit_principal.auth_method,
                    "native method authorization denied"
                );
                Box::pin(async move { Ok(status.into_http()) })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{ErrorDetail, ErrorKind};
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;

    const AUTHN: &str = "/udb.core.authn.services.v1.AuthnService";

    fn decode_detail(status: &Status) -> ErrorDetail {
        let raw = status
            .metadata()
            .get_bin(ERROR_DETAIL_METADATA_KEY)
            .expect("error-detail trailer present")
            .to_bytes()
            .expect("trailer decodes to bytes");
        crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
    }

    /// A transport peer at `ip` (no mTLS), as the TCP acceptor would report it.
    /// Each test uses a distinct IP so the process-global rate-limit buckets
    /// never bleed between tests.
    fn peer_from(ip: &str) -> TransportPeer {
        TransportPeer {
            remote_addr: Some(std::net::SocketAddr::new(
                ip.parse().expect("test peer ip"),
                50051,
            )),
            mtls_client_identity: false,
        }
    }

    #[test]
    fn registry_decodes_endpoint_security_from_descriptor() {
        let reg = method_security_registry();
        assert!(
            !reg.is_empty(),
            "endpoint_security must decode from the embedded descriptor"
        );
        // Public bootstrap RPCs.
        for method in [
            "Authenticate",
            "Login",
            "RefreshToken",
            "GetJwks",
            "ForgotPassword",
            "ResetPassword",
        ] {
            let path = format!("{AUTHN}/{method}");
            let sec = reg
                .get(&path)
                .unwrap_or_else(|| panic!("missing security for {path}"));
            assert_eq!(sec.mode, AuthMode::Public, "{method} should be public");
        }
        // Admin/user RPCs are not public.
        for method in ["CreateUser", "UpdateUser", "ChangeUserStatus"] {
            let path = format!("{AUTHN}/{method}");
            let sec = reg
                .get(&path)
                .unwrap_or_else(|| panic!("missing security for {path}"));
            assert_ne!(sec.mode, AuthMode::Public, "{method} must not be public");
        }
    }

    #[test]
    fn admin_scope_snapshot_preserves_current_control_plane_baseline() {
        assert_eq!(
            ADMIN_SCOPES,
            ["*", "udb:*", "udb:admin", "udb:auth:admin"],
            "Phase E snapshots the current broad native-control-plane admin scopes before later enterprise least-privilege work"
        );
    }

    #[test]
    fn registry_covers_storage_asset_and_webrtc_native_services() {
        let reg = method_security_registry();
        for path in [
            "/udb.core.storage.services.v1.StorageService/RegisterUpload",
            "/udb.core.asset.services.v1.AssetService/CreatePipelineDefinition",
            "/udb.core.webrtc.services.v1.RoomService/CreateRoom",
            "/udb.core.webrtc.services.v1.PeerService/JoinRoom",
            "/udb.core.webrtc.services.v1.TrackService/PublishTrack",
            "/udb.core.webrtc.services.v1.TurnService/IssueCredentials",
            "/udb.core.webrtc.services.v1.SignalingService/Signal",
        ] {
            let sec = reg
                .get(path)
                .unwrap_or_else(|| panic!("{path} missing from method-security registry"));
            assert_ne!(sec.mode, AuthMode::Public, "{path} must not be public");
            assert!(
                !sec.scopes.is_empty() || !sec.roles.is_empty(),
                "{path} should carry an authz gate in endpoint_security"
            );
        }
    }

    #[test]
    fn registry_decodes_descriptor_credential_type_requirements() {
        let reg = method_security_registry();
        let create_user = reg
            .get(&format!("{AUTHN}/CreateUser"))
            .expect("CreateUser security must decode");
        assert!(
            create_user
                .allowed_credential_types
                .contains(&(CredentialType::BearerJwt as i32)),
            "CreateUser must allow bearer JWTs from the descriptor"
        );
        assert!(
            create_user
                .allowed_credential_types
                .contains(&(CredentialType::Session as i32)),
            "CreateUser must allow session-origin JWTs from the descriptor"
        );

        let validate_token = reg
            .get(&format!("{AUTHN}/ValidateToken"))
            .expect("ValidateToken security must decode");
        assert!(
            validate_token
                .allowed_credential_types
                .contains(&(CredentialType::Mtls as i32)),
            "ValidateToken must explicitly allow registered mTLS principals"
        );

        let control_stream = reg
            .get("/udb.core.control.services.v1.ControlPlaneService/StreamResources")
            .expect("ControlPlaneService.StreamResources security must decode");
        assert!(
            control_stream
                .allowed_credential_types
                .contains(&(CredentialType::ServiceAccount as i32)),
            "StreamResources must preserve service-account credential annotations"
        );
    }

    #[test]
    fn public_methods_need_no_bearer() {
        let security = SecurityConfig::current();
        let headers = http::HeaderMap::new();
        for method in ["Login", "RefreshToken"] {
            assert!(
                enforce(
                    &security,
                    &format!("{AUTHN}/{method}"),
                    &headers,
                    &peer_from("198.51.100.7"),
                    None,
                )
                .is_ok(),
                "public {method} must be reachable without a bearer"
            );
        }
    }

    #[test]
    fn public_methods_are_rate_limited_by_transport_peer() {
        let security = SecurityConfig::current();
        let path = format!("{AUTHN}/ForgotPassword");
        let peer = peer_from("203.0.113.9");
        let mut headers = http::HeaderMap::new();
        let limit = public_bootstrap_rate_limit_per_minute();
        for i in 0..limit {
            // Rotate the forgeable forwarded header every request: with no
            // trusted gateway declared it must NOT mint a fresh bucket — the
            // transport peer stays the rate-limit identity.
            headers.insert(
                "x-forwarded-for",
                http::HeaderValue::from_str(&format!("10.9.{}.{}", i / 250, i % 250))
                    .expect("test header"),
            );
            enforce(&security, &path, &headers, &peer, None)
                .expect("within public bootstrap limit");
        }
        let (err, reason) = enforce(&security, &path, &headers, &peer, None)
            .expect_err("public bootstrap limit exceeded");
        assert_eq!(err.code(), tonic::Code::ResourceExhausted);
        assert_eq!(reason, deny_reason::PUBLIC_RATE_LIMIT);
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Quota as i32);
        assert!(detail.retryable);
        assert!(detail.retry_after_ms >= 1_000);
        assert!(detail.retry_after_ms <= 60_000);
        assert_eq!(detail.backend, "method_security");
        assert_eq!(detail.operation, "public_bootstrap_rate_limit");
    }

    #[test]
    fn request_context_required_status_carries_field_violation() {
        let err = request_context_required_status();

        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            err.message(),
            "request context required: send x-correlation-id, x-request-id, or traceparent"
        );
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, "request_context");
        assert_eq!(
            detail.field_violations[0].description,
            "must include x-correlation-id, x-request-id, or traceparent"
        );
    }

    #[test]
    fn method_security_policy_denial_carries_policy_detail() {
        let err = method_security_policy_denied(
            deny_reason::SCOPE,
            "scope udb:admin or udb:auth:admin is required",
        );

        assert_eq!(err.code(), tonic::Code::PermissionDenied);
        assert_eq!(
            err.message(),
            "scope udb:admin or udb:auth:admin is required"
        );
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Policy as i32);
        assert_eq!(detail.operation, "method_security");
        assert_eq!(detail.policy_decision_id, deny_reason::SCOPE);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
        assert!(detail.field_violations.is_empty());
    }

    #[test]
    fn rate_limit_caller_identity_ignores_forgeable_headers_without_trusted_gateway() {
        let mut headers = http::HeaderMap::new();
        headers.insert(
            "x-forwarded-for",
            http::HeaderValue::from_static("1.2.3.4, 5.6.7.8"),
        );
        headers.insert("x-real-ip", http::HeaderValue::from_static("9.9.9.9"));
        let peer = peer_from("198.51.100.20");
        // No trusted gateway declared: forgeable headers never select the bucket.
        assert_eq!(
            rate_limit_caller_id(&headers, &peer, false),
            "198.51.100.20"
        );
        // Trusted gateway declared: the first forwarded hop is the caller.
        assert_eq!(rate_limit_caller_id(&headers, &peer, true), "1.2.3.4");
        // Headers absent: the transport peer is the identity either way — no
        // shared "unknown" bucket for transport-served requests.
        let empty = http::HeaderMap::new();
        assert_eq!(rate_limit_caller_id(&empty, &peer, false), "198.51.100.20");
        assert_eq!(rate_limit_caller_id(&empty, &peer, true), "198.51.100.20");
        // No transport peer at all (non-transport invocation): one shared closed
        // bucket rather than a mintable identity.
        assert_eq!(
            rate_limit_caller_id(&empty, &TransportPeer::default(), false),
            "unknown"
        );
    }

    #[test]
    fn public_bootstrap_rate_limit_is_configurable_with_safe_fallback() {
        assert_eq!(
            parse_public_bootstrap_rate_limit(None),
            DEFAULT_PUBLIC_BOOTSTRAP_RATE_LIMIT_PER_MINUTE
        );
        assert_eq!(parse_public_bootstrap_rate_limit(Some("120")), 120);
        assert_eq!(parse_public_bootstrap_rate_limit(Some(" 5 ")), 5);
        // Zero/garbage must fall back to the named default, never to unlimited.
        assert_eq!(
            parse_public_bootstrap_rate_limit(Some("0")),
            DEFAULT_PUBLIC_BOOTSTRAP_RATE_LIMIT_PER_MINUTE
        );
        assert_eq!(
            parse_public_bootstrap_rate_limit(Some("not-a-number")),
            DEFAULT_PUBLIC_BOOTSTRAP_RATE_LIMIT_PER_MINUTE
        );
    }

    #[test]
    fn internal_peer_requires_loopback_or_mtls_identity() {
        // Loopback peers (v4 + v6) are internal.
        assert!(internal_peer_allowed(&peer_from("127.0.0.1")));
        assert!(internal_peer_allowed(&peer_from("::1")));
        // A TLS-verified mTLS client identity is internal regardless of address.
        assert!(internal_peer_allowed(&TransportPeer {
            remote_addr: Some("203.0.113.50:443".parse().expect("addr")),
            mtls_client_identity: true,
        }));
        // A plain remote client is NOT internal — the gate no longer passes for
        // every gRPC caller (the old content-type check did).
        assert!(!internal_peer_allowed(&peer_from("203.0.113.50")));
        // No resolvable peer fails closed.
        assert!(!internal_peer_allowed(&TransportPeer::default()));
    }

    #[test]
    fn internal_grpc_only_gate_denies_external_callers() {
        // Mirror an RPC whose descriptor sets `internal_grpc_only` — e.g. the
        // control-plane xDS push streams (StreamResources / DeltaResources). Once
        // Gate-C (buf) regenerates the embedded descriptor, `method_security`
        // returns a MethodSecurity with this flag set for those paths and the
        // production `enforce` path runs exactly this gate; this test exercises the
        // single enforcement point directly so it does not depend on regen order.
        let mut internal_only =
            synthetic_method_security(&[CredentialType::BearerJwt, CredentialType::ServiceAccount]);
        internal_only.internal_grpc_only = true;

        // A plain remote caller is rejected, fail closed.
        let (err, reason) =
            enforce_internal_grpc_only(Some(&internal_only), &peer_from("203.0.113.50"))
                .expect_err("internal-only RPC must reject a non-internal peer");
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
        assert_eq!(reason, deny_reason::INTERNAL_ONLY);

        // No resolvable peer (non-transport invocation) also fails closed.
        assert!(
            enforce_internal_grpc_only(Some(&internal_only), &TransportPeer::default()).is_err(),
            "internal-only RPC must fail closed with no resolvable peer"
        );

        // A loopback data-plane node is internal and allowed.
        enforce_internal_grpc_only(Some(&internal_only), &peer_from("127.0.0.1"))
            .expect("loopback node is an internal caller");
        // An mTLS-verified node is internal regardless of its address.
        enforce_internal_grpc_only(
            Some(&internal_only),
            &TransportPeer {
                remote_addr: Some("203.0.113.50:443".parse().expect("addr")),
                mtls_client_identity: true,
            },
        )
        .expect("mTLS-verified node is an internal caller");
    }

    #[test]
    fn non_internal_methods_are_not_gated_by_internal_grpc_only() {
        // An RPC that does NOT set the flag is reachable by any peer through this
        // gate (the rest of `enforce` still applies bearer/scope checks). This is
        // the guard that keeps every SDK-reachable RPC working — only RPCs that
        // explicitly annotate internal_grpc_only are restricted.
        let open = synthetic_method_security(&[CredentialType::BearerJwt]);
        enforce_internal_grpc_only(Some(&open), &peer_from("203.0.113.50"))
            .expect("a non-internal RPC must not be gated");
        enforce_internal_grpc_only(None, &TransportPeer::default())
            .expect("an absent declaration must not be gated here");
    }

    #[test]
    fn non_public_method_requires_bearer() {
        let security = SecurityConfig::current();
        let headers = http::HeaderMap::new();
        let (err, reason) = enforce(
            &security,
            &format!("{AUTHN}/CreateUser"),
            &headers,
            &TransportPeer::default(),
            None,
        )
        .expect_err("CreateUser must require a bearer");
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
        assert_eq!(reason, deny_reason::MISSING_BEARER);
    }

    #[test]
    fn unannotated_path_fails_closed() {
        let security = SecurityConfig::current();
        let headers = http::HeaderMap::new();
        let (err, reason) = enforce(
            &security,
            "/unknown.Service/Method",
            &headers,
            &TransportPeer::default(),
            None,
        )
        .expect_err("unknown methods must fail closed");
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
        assert_eq!(reason, deny_reason::MISSING_BEARER);
    }

    fn synthetic_method_security(allowed: &[CredentialType]) -> MethodSecurity {
        MethodSecurity {
            mode: AuthMode::Bearer,
            roles: Vec::new(),
            scopes: Vec::new(),
            tenant_required: false,
            csrf_required: false,
            internal_grpc_only: false,
            allowed_credential_types: allowed
                .iter()
                .map(|credential| *credential as i32)
                .collect(),
            tenant_field: String::new(),
            project_field: String::new(),
            request_context_required: false,
            policy_ref: None,
            decision_resource: None,
            required_assurance_level: 0,
            owner_field: None,
            rate_limit_policy_ref: None,
            audit_event_type: None,
        }
    }

    fn claims_with(auth_method: &str, service_identity: &str) -> SecurityClaims {
        SecurityClaims {
            tenant_id: Some("tenant-1".to_string()),
            purpose: None,
            scopes: Some(vec!["udb:admin".to_string()]),
            scope: None,
            service_identity: if service_identity.is_empty() {
                None
            } else {
                Some(service_identity.to_string())
            },
            project_id: Some("project-1".to_string()),
            exp: None,
            iat: None,
            sub: Some("subject-1".to_string()),
            iss: None,
            jti: Some("token-1".to_string()),
            roles: None,
            relationships_version: None,
            auth_method: if auth_method.is_empty() {
                None
            } else {
                Some(auth_method.to_string())
            },
            acr: None,
            account_kind: None,
        }
    }

    #[test]
    fn bearer_claim_auth_method_drives_descriptor_credential_type() {
        assert_eq!(
            credential_type_for_bearer_claims(&claims_with("jwt", "")),
            CredentialType::BearerJwt
        );
        assert_eq!(
            credential_type_for_bearer_claims(&claims_with("session", "")),
            CredentialType::Session
        );
        assert_eq!(
            credential_type_for_bearer_claims(&claims_with("api_key", "")),
            CredentialType::ApiKey
        );
        assert_eq!(
            credential_type_for_bearer_claims(&claims_with("service_account", "svc:node")),
            CredentialType::ServiceAccount
        );
    }

    #[test]
    fn session_origin_jwt_satisfies_session_only_descriptor() {
        let security = synthetic_method_security(&[CredentialType::Session]);
        enforce_bearer_credential_type(Some(&security), &claims_with("session", ""))
            .expect("session-origin JWT should satisfy a session credential contract");
    }

    #[test]
    fn api_key_origin_jwt_fails_closed_when_descriptor_omits_api_key() {
        let security =
            synthetic_method_security(&[CredentialType::BearerJwt, CredentialType::Session]);
        let (err, reason) =
            enforce_bearer_credential_type(Some(&security), &claims_with("api_key", ""))
                .expect_err("API-key-origin JWT must not satisfy bearer/session-only contract");
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
        assert_eq!(reason, deny_reason::CREDENTIAL_TYPE);
    }

    #[test]
    fn service_account_jwt_requires_service_account_descriptor() {
        let user_bearer_only = synthetic_method_security(&[CredentialType::BearerJwt]);
        let (err, reason) = enforce_bearer_credential_type(
            Some(&user_bearer_only),
            &claims_with("service_account", "svc:control-plane"),
        )
        .expect_err("service-account JWT must not satisfy a bearer-only contract");
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
        assert_eq!(reason, deny_reason::CREDENTIAL_TYPE);

        let service_account = synthetic_method_security(&[CredentialType::ServiceAccount]);
        enforce_bearer_credential_type(
            Some(&service_account),
            &claims_with("service_account", "svc:control-plane"),
        )
        .expect("service-account JWT should satisfy a service-account credential contract");
    }

    fn mtls_credentials(
        scopes: &[&str],
    ) -> crate::runtime::credential_layer::PreresolvedCredentials {
        crate::runtime::credential_layer::PreresolvedCredentials {
            certificate_identity: Some("spiffe://test.udb/workload".to_string()),
            certificate_present: true,
            certificate_principal: Some(crate::runtime::credential_layer::VerifiedPrincipal {
                credential_type: CredentialType::Mtls as i32,
                subject: "service-account-a".to_string(),
                service_identity: "spiffe://test.udb/workload".to_string(),
                tenant_id: "tenant-a".to_string(),
                project_id: "project-a".to_string(),
                scopes: scopes.iter().map(|scope| (*scope).to_string()).collect(),
                credential_id: "binding-a".to_string(),
                auth_method: "mtls".to_string(),
                certificate_identity: Some("spiffe://test.udb/workload".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn registered_mtls_runs_the_common_scope_and_tenant_gates() {
        let security = SecurityConfig::current();
        let path = format!("{AUTHN}/ValidateToken");
        let peer = peer_from("198.51.100.61");
        let mut headers = http::HeaderMap::new();
        headers.insert(
            "x-correlation-id",
            http::HeaderValue::from_static("request-a"),
        );
        headers.insert("x-tenant-id", http::HeaderValue::from_static("tenant-a"));

        let allowed = mtls_credentials(&["udb:authn:validate-token"]);
        let (principal, claim) = enforce(&security, &path, &headers, &peer, Some(&allowed))
            .expect("descriptor-approved mTLS with the declared scope must pass");
        assert_eq!(principal.auth_method, "mtls");
        assert_eq!(principal.credential_type, CredentialType::Mtls as i32);
        assert_eq!(principal.credential_id, "binding-a");
        assert_eq!(principal.service_identity, "spiffe://test.udb/workload");
        assert_eq!(claim.tenant_id, "tenant-a");
        assert_eq!(claim.credential_id, "binding-a");
        assert_eq!(claim.service_identity, "spiffe://test.udb/workload");

        let missing_scope = mtls_credentials(&["udb:read"]);
        let (error, reason) = enforce(&security, &path, &headers, &peer, Some(&missing_scope))
            .expect_err("mTLS must not return before the common scope gate");
        assert_eq!(error.code(), tonic::Code::PermissionDenied);
        assert_eq!(reason, deny_reason::SCOPE);

        headers.insert("x-tenant-id", http::HeaderValue::from_static("tenant-b"));
        let (error, reason) = enforce(&security, &path, &headers, &peer, Some(&allowed))
            .expect_err("mTLS must not return before the common tenant gate");
        assert_eq!(error.code(), tonic::Code::PermissionDenied);
        assert_eq!(reason, deny_reason::TENANT_MISMATCH);
    }

    #[test]
    fn native_bearer_and_certificate_require_identical_lineage() {
        let security = SecurityConfig::current();
        let path = format!("{AUTHN}/ValidateToken");
        let peer = peer_from("198.51.100.62");
        let mut headers = http::HeaderMap::new();
        headers.insert(
            "authorization",
            http::HeaderValue::from_static("Bearer already-verified"),
        );
        headers.insert("x-tenant-id", http::HeaderValue::from_static("tenant-a"));
        // ValidateToken requires request context (request_context_required); the
        // sibling registered-mtls tests supply it too.
        headers.insert(
            "x-correlation-id",
            http::HeaderValue::from_static("request-lineage"),
        );
        let mut credentials = mtls_credentials(&["udb:authn:validate-token"]);
        credentials.bearer = Some(Ok(crate::runtime::credential_layer::VerifiedPrincipal {
            credential_type: CredentialType::BearerJwt as i32,
            subject: "service-account-a".to_string(),
            service_identity: "spiffe://test.udb/workload".to_string(),
            tenant_id: "tenant-a".to_string(),
            project_id: "project-a".to_string(),
            scopes: vec!["udb:authn:validate-token".to_string()],
            credential_id: "jwt-a".to_string(),
            auth_method: "password".to_string(),
            ..Default::default()
        }));
        enforce(&security, &path, &headers, &peer, Some(&credentials))
            .expect("matching bearer/certificate lineage must compose");

        credentials
            .certificate_principal
            .as_mut()
            .expect("certificate principal")
            .project_id = "project-b".to_string();
        let (error, reason) = enforce(&security, &path, &headers, &peer, Some(&credentials))
            .expect_err("native listener must reject bearer/certificate splicing");
        assert_eq!(error.code(), tonic::Code::PermissionDenied);
        assert_eq!(reason, deny_reason::CREDENTIAL_STATE);
    }

    #[test]
    fn registered_mtls_is_denied_when_descriptor_does_not_opt_in() {
        let security = SecurityConfig::current();
        let mut headers = http::HeaderMap::new();
        headers.insert(
            "x-correlation-id",
            http::HeaderValue::from_static("request-b"),
        );
        let credentials = mtls_credentials(&["udb:authn:user:create"]);
        let (error, reason) = enforce(
            &security,
            &format!("{AUTHN}/CreateUser"),
            &headers,
            &peer_from("198.51.100.62"),
            Some(&credentials),
        )
        .expect_err("mTLS requires an explicit descriptor credential-type opt-in");
        assert_eq!(error.code(), tonic::Code::Unauthenticated);
        assert_eq!(reason, deny_reason::MISSING_BEARER);
    }

    #[test]
    fn direct_session_header_fails_closed_until_exchanged_for_jwt() {
        let security = SecurityConfig::current();
        let mut headers = http::HeaderMap::new();
        headers.insert("x-udb-session", http::HeaderValue::from_static("sess_raw"));
        let (err, reason) = enforce(
            &security,
            &format!("{AUTHN}/CreateUser"),
            &headers,
            &TransportPeer::default(),
            None,
        )
        .expect_err("raw session credentials must not bypass JWT validation");
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
        assert_eq!(reason, deny_reason::CREDENTIAL_TYPE);
    }

    #[test]
    fn direct_api_key_header_fails_closed_when_method_omits_api_key() {
        let security = SecurityConfig::current();
        let mut headers = http::HeaderMap::new();
        headers.insert(
            "x-api-key",
            http::HeaderValue::from_static("udbk_prefix.secret"),
        );
        let (err, reason) = enforce(
            &security,
            &format!("{AUTHN}/CreateUser"),
            &headers,
            &TransportPeer::default(),
            None,
        )
        .expect_err("raw API keys must not be accepted for bearer/session methods");
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
        assert_eq!(reason, deny_reason::CREDENTIAL_TYPE);
    }

    // A trivial inner service standing in for a tonic-generated server: it always
    // returns `grpc-status: 0`, so a `0` in the response proves the layer let the
    // request reach the inner service (passed), while a non-`0` proves the layer
    // short-circuited with a `Status` (rejected) before the inner ran.
    #[derive(Clone)]
    struct Ok200;

    impl Service<http::Request<BoxBody>> for Ok200 {
        type Response = http::Response<BoxBody>;
        type Error = std::convert::Infallible;
        type Future =
            Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: http::Request<BoxBody>) -> Self::Future {
            Box::pin(async {
                Ok(http::Response::builder()
                    .header("grpc-status", "0")
                    .body(tonic::codegen::empty_body())
                    .unwrap())
            })
        }
    }

    async fn grpc_status_for(path: &str, bearer: Option<&str>) -> String {
        let mut svc = MethodSecurityLayer::new().wrap(Ok200);
        let mut builder = http::Request::builder().uri(path);
        if let Some(token) = bearer {
            builder = builder.header("authorization", format!("Bearer {token}"));
        }
        let req = builder.body(tonic::codegen::empty_body()).unwrap();
        std::future::poll_fn(|cx| svc.poll_ready(cx))
            .await
            .expect("ready");
        let resp = svc.call(req).await.expect("call");
        resp.headers()
            .get("grpc-status")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string()
    }

    #[tokio::test]
    async fn layer_passes_public_rpc_without_bearer() {
        // grpc-status 0 means the request reached the inner service.
        let status = grpc_status_for(&format!("{AUTHN}/Authenticate"), None).await;
        assert_eq!(
            status, "0",
            "public Authenticate must reach the inner service"
        );
    }

    #[tokio::test]
    async fn layer_rejects_non_public_rpc_without_bearer() {
        // 16 == UNAUTHENTICATED; the layer must short-circuit before the inner.
        let status = grpc_status_for(&format!("{AUTHN}/CreateUser"), None).await;
        assert_eq!(
            status, "16",
            "non-public CreateUser without a bearer must be rejected by the layer"
        );
    }

    #[tokio::test]
    async fn layer_records_denial_metric_with_reason() {
        // Phase 10: a denial must increment udb_method_security_denials_total with
        // the stable reason label, via the wired metrics recorder.
        let metrics =
            Arc::new(crate::metrics::PrometheusMetrics::new().expect("build PrometheusMetrics"));
        let layer = MethodSecurityLayer::new().with_metrics(metrics.clone());
        let mut svc = layer.wrap(Ok200);
        let req = http::Request::builder()
            .uri(format!("{AUTHN}/CreateUser"))
            .body(tonic::codegen::empty_body())
            .unwrap();
        std::future::poll_fn(|cx| svc.poll_ready(cx))
            .await
            .expect("ready");
        let _ = svc.call(req).await.expect("call");
        let text = metrics.gather_text("");
        assert!(
            text.contains("udb_method_security_denials_total{reason=\"missing_bearer\"} 1"),
            "denial counter not incremented:\n{text}"
        );
    }

    fn claim_ctx(
        subject: &str,
        tenant: &str,
        project: &str,
        scopes: &[&str],
        roles: &[&str],
    ) -> VerifiedClaimContext {
        VerifiedClaimContext {
            subject: subject.to_string(),
            tenant_id: tenant.to_string(),
            project_id: project.to_string(),
            scopes: scopes.iter().map(|s| s.to_string()).collect(),
            roles: roles.iter().map(|r| r.to_string()).collect(),
            authenticated: true,
            ..VerifiedClaimContext::default()
        }
    }

    /// Run `f` with `ctx` installed as the current claim context, exactly as the
    /// tower layer does for a real request — so [`claim_context_present`] sees it
    /// and the D1/D2 guards enforce (rather than skipping the in-process path).
    fn with_ctx<R>(ctx: &VerifiedClaimContext, f: impl FnOnce() -> R) -> R {
        CURRENT_CLAIM_CONTEXT.sync_scope(ctx.clone(), f)
    }

    // ── C21: assurance-level mapping (the AAL step-up gate's comparator) ──────

    #[test]
    fn aal_level_maps_acr_strings() {
        assert_eq!(aal_level("aal1"), 1);
        assert_eq!(aal_level("AAL2"), 2);
        assert_eq!(aal_level("aal3"), 3);
        assert_eq!(aal_level(""), 1);
        assert_eq!(aal_level("garbage"), 1);
        // A required level of 2 denies an aal1 caller, admits an aal2 caller.
        assert!(aal_level("aal1") < 2);
        assert!(aal_level("aal2") >= 2);
    }

    // ── C23: body-owner-vs-principal ─────────────────────────────────────────

    #[test]
    fn body_owner_matching_principal_is_allowed() {
        let ctx = claim_ctx("u-1", "tenant-a", "proj-1", &["udb:write"], &[]);
        with_ctx(&ctx, || {
            enforce_body_owner_matches_claim(&ctx, "u-1").expect("same owner allowed");
        });
    }

    #[test]
    fn body_owner_mismatch_is_denied() {
        let ctx = claim_ctx("u-1", "tenant-a", "proj-1", &["udb:write"], &[]);
        with_ctx(&ctx, || {
            let err = enforce_body_owner_matches_claim(&ctx, "someone-else").unwrap_err();
            assert_eq!(err.code(), tonic::Code::PermissionDenied);
        });
    }

    #[test]
    fn body_owner_empty_inherits_principal() {
        let ctx = claim_ctx("u-1", "tenant-a", "proj-1", &["udb:write"], &[]);
        with_ctx(&ctx, || {
            enforce_body_owner_matches_claim(&ctx, "").expect("empty owner inherits principal");
        });
    }

    // ── C24: named rate-limit policy → distinct bucket + tunable limit ────────

    #[test]
    fn rate_limit_policy_env_key_normalizes() {
        assert_eq!(
            rate_limit_policy_env_key("authn.login.public"),
            "UDB_RATE_LIMIT_POLICY_AUTHN_LOGIN_PUBLIC"
        );
    }

    #[test]
    fn public_rate_limit_key_differs_by_policy_ref() {
        let headers = http::HeaderMap::new();
        let peer = TransportPeer::default();
        let a = public_rate_limit_key("/svc/M", &headers, &peer, Some("policy.a"));
        let b = public_rate_limit_key("/svc/M", &headers, &peer, Some("policy.b"));
        let none = public_rate_limit_key("/svc/M", &headers, &peer, None);
        assert_ne!(a.0, b.0, "distinct policies must get distinct buckets");
        assert_eq!(none.0, "/svc/M");
        assert!(a.0.contains("policy.a"));
    }

    #[test]
    fn unset_policy_falls_back_to_default_limit() {
        assert_eq!(
            public_bootstrap_rate_limit_for(None),
            public_bootstrap_rate_limit_per_minute()
        );
        assert_eq!(
            public_bootstrap_rate_limit_for(Some("never.configured.policy.xyz")),
            public_bootstrap_rate_limit_per_minute()
        );
    }

    // ── L2: every endpoint_security field is enforced or whitelisted ──────────

    /// Anti-drift guard. `method_security_from_contract` is the ONLY runtime copy
    /// of `endpoint_security` on the request path, so a field it drops is
    /// unenforceable — the seam that made C21/C23/C24/C25 decorative. This test
    /// pins that every contract field is either PROJECTED (enforced) or documented
    /// metadata-only, and the exhaustive destructure at the end makes adding a new
    /// field a COMPILE error until the author classifies it.
    #[test]
    fn every_endpoint_security_field_is_enforced_or_whitelisted() {
        let es = EndpointSecurityContract {
            mode: 2,
            roles: vec!["r".into()],
            scopes: vec!["s".into()],
            tenant_required: true,
            csrf_required: true,
            policy_ref: "p".into(),
            internal_grpc_only: true,
            required_assurance_level: 2,
            allowed_credential_types: vec![1],
            rate_limit_policy_ref: "rl".into(),
            abuse_policy_ref: "ab".into(),
            audit_event_type: "ae".into(),
            decision_resource: "dr".into(),
            owner_field: "owner".into(),
            tenant_field: "tf".into(),
            project_field: "pf".into(),
            idempotency_required: true,
            request_context_required: true,
        };
        let ms = method_security_from_contract(&es);
        // Enforced (projected + wired) — every gate field must round-trip.
        assert_eq!(ms.roles, vec!["r".to_string()]);
        assert_eq!(ms.scopes, vec!["s".to_string()]);
        assert!(ms.tenant_required);
        assert!(ms.csrf_required);
        assert!(ms.internal_grpc_only);
        assert_eq!(ms.allowed_credential_types, vec![1]);
        assert_eq!(ms.tenant_field, "tf");
        assert_eq!(ms.project_field, "pf");
        assert!(ms.request_context_required);
        assert_eq!(ms.policy_ref.as_deref(), Some("p"));
        assert_eq!(ms.decision_resource.as_deref(), Some("dr"));
        // Newly wired — must no longer be dropped:
        assert_eq!(ms.required_assurance_level, 2, "C21 field dropped");
        assert_eq!(
            ms.owner_field.as_deref(),
            Some("owner"),
            "C23 field dropped"
        );
        assert_eq!(
            ms.rate_limit_policy_ref.as_deref(),
            Some("rl"),
            "C24 field dropped"
        );
        assert_eq!(
            ms.audit_event_type.as_deref(),
            Some("ae"),
            "C25 field dropped"
        );

        // Exhaustive destructure — adding a field to EndpointSecurityContract makes
        // THIS fail to compile until the author decides: wire it into
        // `method_security_from_contract` + assert it above (enforced), or add it to
        // the metadata-only whitelist here with a rationale.
        let EndpointSecurityContract {
            // ── enforced (projected into MethodSecurity) ──
            mode: _,
            roles: _,
            scopes: _,
            tenant_required: _,
            csrf_required: _,
            policy_ref: _,
            internal_grpc_only: _,
            required_assurance_level: _,
            allowed_credential_types: _,
            rate_limit_policy_ref: _,
            audit_event_type: _,
            decision_resource: _,
            owner_field: _,
            tenant_field: _,
            project_field: _,
            request_context_required: _,
            // ── metadata-only whitelist (intentionally NOT a MethodSecurity gate) ──
            // abuse_policy_ref: reserved paired signal; no consumer yet.
            abuse_policy_ref: _,
            // idempotency_required: superseded by the separate
            // method_idempotency_contract (own posture gate + dedup).
            idempotency_required: _,
        } = &es;
    }

    // ── D1: body-tenant-vs-claim ─────────────────────────────────────────────

    #[test]
    fn body_tenant_matching_claim_is_allowed() {
        let ctx = claim_ctx("u-1", "tenant-a", "proj-1", &["udb:authn:write"], &[]);
        with_ctx(&ctx, || {
            enforce_body_tenant_matches_claim(&ctx, "tenant-a", "proj-1")
                .expect("same-tenant body must be allowed");
        });
    }

    #[test]
    fn body_tenant_b_with_tenant_a_token_is_denied() {
        // The core D1 attack: tenant-A admin token, body tenant_id=tenant-b.
        let ctx = claim_ctx("u-1", "tenant-a", "", &["udb:authn:write"], &[]);
        with_ctx(&ctx, || {
            let err = enforce_body_tenant_matches_claim(&ctx, "tenant-b", "")
                .expect_err("cross-tenant body must be denied");
            assert_eq!(err.code(), tonic::Code::PermissionDenied);
            let detail = decode_detail(&err);
            assert_eq!(detail.kind, ErrorKind::Policy as i32);
            assert_eq!(detail.operation, "method_security");
            assert_eq!(detail.policy_decision_id, deny_reason::TENANT_MISMATCH);
        });
    }

    #[test]
    fn empty_body_tenant_inherits_claim_and_is_allowed() {
        let ctx = claim_ctx("u-1", "tenant-a", "", &["udb:authn:write"], &[]);
        with_ctx(&ctx, || {
            enforce_body_tenant_matches_claim(&ctx, "", "")
                .expect("empty body tenant inherits the claim tenant");
        });
    }

    #[test]
    fn cross_tenant_admin_role_may_target_other_tenant() {
        let ctx = claim_ctx("ops-1", "tenant-a", "project-a", &[], &["platform_admin"]);
        with_ctx(&ctx, || {
            enforce_body_tenant_matches_claim(&ctx, "tenant-b", "project-b")
                .expect("platform admin may act cross-tenant");
        });
    }

    #[test]
    fn explicit_platform_scope_may_target_other_tenant() {
        let ctx = claim_ctx(
            "ops-1",
            "tenant-a",
            "project-a",
            &["udb:platform_admin"],
            &[],
        );
        with_ctx(&ctx, || {
            enforce_body_tenant_matches_claim(&ctx, "tenant-b", "project-b")
                .expect("explicit platform scope may act cross-tenant");
        });
    }

    #[test]
    fn broad_admin_scope_may_target_other_tenant() {
        let ctx = claim_ctx("ops-1", "", "", &["udb:admin"], &[]);
        with_ctx(&ctx, || {
            enforce_body_tenant_matches_claim(&ctx, "tenant-b", "")
                .expect("unbound control-plane admin scope may act cross-tenant");
        });
    }

    #[test]
    fn bound_broad_admin_scopes_cannot_erase_tenant_or_project_claims() {
        for scope in ["*", "udb:*", "udb:admin", "udb:auth:admin"] {
            let ctx = claim_ctx("ops-1", "tenant-a", "project-a", &[scope], &[]);
            assert!(
                !ctx.is_cross_tenant_admin(),
                "bound broad scope {scope} must not become platform authority"
            );
            with_ctx(&ctx, || {
                let tenant_err = enforce_body_tenant_matches_claim(&ctx, "tenant-b", "project-a")
                    .expect_err("bound broad admin must not cross tenants");
                assert_eq!(tenant_err.code(), tonic::Code::PermissionDenied);

                let project_err = enforce_body_tenant_matches_claim(&ctx, "tenant-a", "project-b")
                    .expect_err("bound broad admin must not cross projects");
                assert_eq!(project_err.code(), tonic::Code::PermissionDenied);
            });

            let project_only = claim_ctx("ops-1", "", "project-a", &[scope], &[]);
            assert!(
                !project_only.is_cross_tenant_admin(),
                "project-only broad scope {scope} must not become platform authority"
            );
            with_ctx(&project_only, || {
                let err = enforce_body_tenant_matches_claim(&project_only, "tenant-a", "project-a")
                    .expect_err("request body cannot supply a missing claim tenant");
                assert_eq!(err.code(), tonic::Code::PermissionDenied);
            });
        }
    }

    #[test]
    fn tenantless_non_admin_caller_is_denied() {
        // A token with no tenant and no cross-tenant admin cannot act tenant-wide.
        let ctx = claim_ctx("u-1", "", "", &["udb:authn:write"], &[]);
        with_ctx(&ctx, || {
            let err = enforce_body_tenant_matches_claim(&ctx, "", "")
                .expect_err("tenantless non-admin must be denied");
            assert_eq!(err.code(), tonic::Code::PermissionDenied);
        });
    }

    #[test]
    fn resolve_body_tenant_scope_inherits_claim_when_body_omits_tenant() {
        let ctx = claim_ctx("u-1", "tenant-a", "proj-1", &["udb:authn:write"], &[]);
        with_ctx(&ctx, || {
            let (tenant, project) = resolve_body_tenant_scope(&ctx, "", "")
                .expect("empty body tenant/project must inherit the verified claim");
            assert_eq!(tenant, "tenant-a");
            assert_eq!(project, "proj-1");
        });
    }

    #[test]
    fn resolve_body_tenant_scope_rejects_body_claim_mismatch() {
        let ctx = claim_ctx("u-1", "tenant-a", "proj-1", &["udb:authn:write"], &[]);
        with_ctx(&ctx, || {
            let err = resolve_body_tenant_scope(&ctx, "tenant-b", "proj-1")
                .expect_err("foreign body tenant must be rejected");
            assert_eq!(err.code(), tonic::Code::PermissionDenied);
        });
    }

    #[test]
    fn resolve_body_tenant_scope_rejects_tenantless_non_admin() {
        let ctx = claim_ctx("u-1", "", "", &["udb:authn:write"], &[]);
        with_ctx(&ctx, || {
            let err = resolve_body_tenant_scope(&ctx, "", "")
                .expect_err("tenantless non-admin requests must fail closed");
            assert_eq!(err.code(), tonic::Code::PermissionDenied);
        });
    }

    #[test]
    fn body_project_mismatch_within_tenant_is_denied() {
        let ctx = claim_ctx("u-1", "tenant-a", "proj-1", &["udb:authn:write"], &[]);
        with_ctx(&ctx, || {
            let err = enforce_body_tenant_matches_claim(&ctx, "tenant-a", "proj-2")
                .expect_err("cross-project body must be denied");
            assert_eq!(err.code(), tonic::Code::PermissionDenied);
        });
    }

    #[test]
    fn public_unauthenticated_context_is_never_cross_tenant_admin() {
        let ctx = VerifiedClaimContext::default();
        assert!(!ctx.is_cross_tenant_admin());
        // With an unauthenticated context INSTALLED (as the layer does for public
        // RPCs), the guard cannot pass (both tenants empty, not an admin → denied).
        with_ctx(&ctx, || {
            assert!(enforce_body_tenant_matches_claim(&ctx, "", "").is_err());
        });
    }

    #[test]
    fn guards_skip_when_no_claim_context_is_installed() {
        // The in-process / test path: no context scoped → the guards must not deny
        // (real over-the-wire requests always carry a context from the layer).
        let ctx = VerifiedClaimContext::default();
        assert!(!claim_context_present());
        enforce_body_tenant_matches_claim(&ctx, "tenant-b", "proj-x")
            .expect("absent claim context must skip the body-tenant guard");
        authorize_action(&ctx, "authn.user.create", &["authn.user.create"])
            .expect("absent claim context must skip the per-action guard");
    }

    // ── D2: per-action authz ─────────────────────────────────────────────────

    #[test]
    fn action_scope_present_authorizes_and_returns_decision_id() {
        let ctx = claim_ctx("u-1", "tenant-a", "", &["authn.user.create"], &[]);
        with_ctx(&ctx, || {
            let decision = authorize_action(&ctx, "authn.user.create", &["authn.user.create"])
                .expect("action scope authorizes the mutation");
            assert!(!decision.is_empty(), "a decision id must be recorded");
        });
    }

    #[test]
    fn broad_admin_scope_authorizes_any_action() {
        let ctx = claim_ctx("ops-1", "tenant-a", "", &["udb:admin"], &[]);
        with_ctx(&ctx, || {
            authorize_action(&ctx, "authn.user.update", &["authn.user.update"])
                .expect("broad admin scope keeps legitimate operators working");
        });
    }

    #[test]
    fn missing_action_scope_is_denied() {
        // Authenticated, passed the coarse transport gate, but carries no concrete
        // action scope and no broad admin scope → the per-action guard denies.
        let ctx = claim_ctx("u-1", "tenant-a", "", &["udb:authn:read"], &[]);
        with_ctx(&ctx, || {
            let err = authorize_action(&ctx, "authn.user.create", &["authn.user.create"])
                .expect_err("non-admin without the action scope must be denied");
            assert_eq!(err.code(), tonic::Code::PermissionDenied);
            let detail = decode_detail(&err);
            assert_eq!(detail.kind, ErrorKind::Policy as i32);
            assert_eq!(detail.operation, "method_security");
            assert_eq!(detail.policy_decision_id, deny_reason::SCOPE);
        });
    }

    #[test]
    fn decision_resource_prefers_descriptor_annotation() {
        // CreateUser's endpoint_security declares `decision_resource:
        // "authn.CreateUser"`, decoded from the descriptor into MethodSecurity; the
        // per-action authz decision must authorize against THAT explicit resource.
        let path = format!("{AUTHN}/CreateUser");
        let sec = method_security(&path).expect("CreateUser security must decode");
        assert_eq!(
            sec.decision_resource.as_deref(),
            Some("authn.CreateUser"),
            "policy_ref/decision_resource must be decoded from endpoint_security"
        );
        assert_eq!(decision_resource_for(&path), "authn.CreateUser");
    }

    #[test]
    fn decision_resource_for_unannotated_path_is_synthetic() {
        // An unknown path has no MethodSecurity entry → always the synthetic form.
        let resource = decision_resource_for("/unknown.Service/DoThing");
        assert_eq!(resource, "native.rpc:unknown.Service/DoThing");
        assert!(method_policy_ref("/unknown.Service/DoThing").is_none());
    }

    #[test]
    fn claim_context_projects_into_authz_principal() {
        let ctx = claim_ctx(
            "u-7",
            "tenant-z",
            "proj-z",
            &["authn.user.create"],
            &["editor"],
        );
        let principal = ctx.to_principal();
        assert_eq!(principal.subject, "u-7");
        assert_eq!(principal.tenant_id, "tenant-z");
        assert_eq!(principal.project_id, "proj-z");
        assert!(principal.has_scope("authn.user.create"));
        assert!(principal.roles.contains(&"editor".to_string()));
    }

    #[test]
    fn unauthenticated_action_is_denied() {
        // A public-bootstrap context IS installed but `authenticated=false`.
        let ctx = VerifiedClaimContext::default();
        with_ctx(&ctx, || {
            let err = authorize_action(&ctx, "authn.user.create", &["authn.user.create"])
                .expect_err("unauthenticated principal cannot perform an action");
            assert_eq!(err.code(), tonic::Code::Unauthenticated);
        });
    }

    // Conformance gate (plan F13 / criterion 2): EVERY native control-plane RPC
    // must carry an `endpoint_security` annotation, so runtime auth behavior is
    // proto-driven for every method (no silently-unguarded RPCs). Fails with the
    // exact list of any unannotated RPCs.
    #[test]
    fn every_native_control_plane_rpc_is_annotated() {
        let registry = method_security_registry();
        let mut missing: Vec<String> = all_native_service_rpc_paths()
            .into_iter()
            .filter(|path| !registry.contains_key(path))
            .collect();
        missing.sort();
        assert!(
            !all_native_service_rpc_paths().is_empty(),
            "expected to discover native control-plane RPCs from the descriptor"
        );
        assert!(
            missing.is_empty(),
            "{} native control-plane RPC(s) lack an endpoint_security annotation: {missing:#?}",
            missing.len()
        );
    }
}
