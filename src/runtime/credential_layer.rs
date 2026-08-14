//! Phase-1 verified-principal pipeline.
//!
//! ONE asynchronous credential-resolution pass runs as a Tower layer BEFORE
//! DataBroker handlers or native method security execute, and its result rides
//! the request extensions. This replaces the old `block_in_place` bridge that
//! parked a runtime worker on a synchronous database lookup inside
//! `security_from_request`: the layer awaits the durable-store lookups
//! natively, and the sync header layer only CONSUMES the pre-resolved outcome.
//!
//! The layer NEVER denies a request itself — public/bootstrap RPCs and the
//! health service must keep working, and per-method enforcement (descriptor
//! `endpoint_security`, tenant checks, scope checks) stays where it lives
//! today (`security_from_request` + `MethodSecurityLayer`). It only ANNOTATES:
//! whatever it resolves is authoritative, whatever it cannot resolve is left
//! for the sync layer's fail-closed handling.
//!
//! Dependencies (`AuthPlaneDeps`) are installed at startup INDEPENDENTLY of
//! native-listener enablement, so a DataBroker-only deployment authenticates
//! scoped API keys and registered mTLS certificates exactly like a full one.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock, RwLock};
use std::task::{Context, Poll};

use tonic::body::BoxBody;
use tonic::codegen::http;
use tower::Service;

use crate::runtime::authn::PostgresApiKeyStore;

/// The canonical verified principal: one contract carrying
/// everything enforcement layers need, derived ONLY from verified credentials
/// and server-controlled records. Constructed by [`CredentialResolveLayer`]
/// (async paths) or by `security_from_request` (sync JWT path) — never from
/// caller-asserted headers.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VerifiedPrincipal {
    /// `udb.core.common.v1.CredentialType` numeric value (BEARER_JWT=1,
    /// SESSION=2, API_KEY=3, SERVICE_ACCOUNT=4, MTLS=5).
    pub credential_type: i32,
    /// The verified subject (JWT `sub`, key owner, or bound service account).
    pub subject: String,
    /// Immutable service identity, when the credential carries one.
    pub service_identity: String,
    pub tenant_id: String,
    pub project_id: String,
    pub scopes: Vec<String>,
    pub roles: Vec<String>,
    /// Credential identifier for audit lineage (jti / key prefix / binding id).
    /// Never the secret itself.
    pub credential_id: String,
    pub auth_method: String,
    /// Absolute credential expiry in Unix seconds. `0` means the credential
    /// has no intrinsic expiry and must still be periodically re-resolved by
    /// long-lived served paths so revocation/grant changes take effect.
    pub expires_at_unix: i64,
    /// The certificate-derived identity when a verified peer certificate was
    /// present (also set alongside a bearer/key for composition checks).
    pub certificate_identity: Option<String>,
    /// Per-key data-plane budget from the credential's stored record
    /// (`api_keys.rate_limit_per_minute`). Only meaningful for API-key
    /// principals; `0` for every other credential type (tenant default applies).
    pub rate_limit_per_minute: i64,
}

impl VerifiedPrincipal {
    /// Build the canonical principal from claims that have already passed JWT
    /// verification. Keeping this projection here prevents the async resolver
    /// and direct in-process request path from assigning different credential
    /// lineage to the same bearer.
    pub(crate) fn from_verified_bearer_claims(
        claims: &crate::runtime::security::SecurityClaims,
    ) -> Self {
        let service_identity = claims.service_identity.clone().unwrap_or_default();
        let auth_method = claims
            .auth_method
            .clone()
            .unwrap_or_else(|| "bearer_jwt".to_string());
        let credential_type = if !service_identity.trim().is_empty() {
            4 // CREDENTIAL_TYPE_SERVICE_ACCOUNT
        } else {
            match auth_method.trim().to_ascii_lowercase().as_str() {
                "session" => 2,
                "api_key" | "apikey" => 3,
                "mtls" => 5,
                _ => 1, // CREDENTIAL_TYPE_BEARER_JWT
            }
        };

        Self {
            credential_type,
            subject: claims.sub.clone().unwrap_or_default(),
            service_identity,
            tenant_id: claims.tenant_id.clone().unwrap_or_default(),
            project_id: claims.project_id.clone().unwrap_or_default(),
            scopes: claims.resolved_scopes(),
            roles: claims.roles.clone().unwrap_or_default(),
            credential_id: claims.jti.clone().unwrap_or_default(),
            auth_method,
            expires_at_unix: claims.exp.unwrap_or_default(),
            certificate_identity: None,
            // Bearer/JWT principals carry no api-key budget — tenant default.
            rate_limit_per_minute: 0,
        }
    }
}

/// Secret-bearing evidence retained only so long-lived served calls can
/// periodically re-run the canonical credential resolver. Debug output is
/// deliberately redacted; audit/log paths use [`VerifiedPrincipal`] lineage,
/// never the bearer, API-key secret, or certificate bytes.
#[derive(Clone, Default)]
pub struct CredentialRevalidator {
    bearer: Option<String>,
    api_key: Option<String>,
    peer_cert_der: Option<Vec<u8>>,
}

impl std::fmt::Debug for CredentialRevalidator {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CredentialRevalidator")
            .field("bearer", &self.bearer.is_some())
            .field("api_key", &self.api_key.is_some())
            .field("peer_cert_der", &self.peer_cert_der.is_some())
            .finish()
    }
}

/// Pre-resolved async credential outcomes, inserted into request extensions by
/// [`CredentialResolveLayer`] and consumed by `security_from_request`.
#[derive(Debug, Clone, Default)]
pub struct PreresolvedCredentials {
    /// Verified bearer principal when an Authorization bearer was present.
    /// Validation errors are retained for the method-specific enforcement
    /// layer, which denies without repeating signature verification.
    pub bearer: Option<Result<VerifiedPrincipal, String>>,
    /// Outcome of the durable API-key lookup, when an `x-api-key` header was
    /// present AND the auth-plane deps are installed. `None` = no key header
    /// (or deps not installed → the sync layer fails closed as before).
    pub api_key: Option<Result<Option<VerifiedPrincipal>, String>>,
    /// The verified peer-certificate identity (SPIFFE/DNS SAN or CN), when a
    /// client certificate was presented — resolved whether or not another
    /// credential is present, so composition checks can use it.
    pub certificate_identity: Option<String>,
    /// True whenever the verified TLS peer supplied a certificate, including
    /// certificates with no parseable identity. Consumers use this separately
    /// from `certificate_identity` so malformed or unbound certificates cannot
    /// disappear when a bearer or API key is also present.
    pub certificate_present: bool,
    /// The server-controlled principal for the certificate, resolved through
    /// the durable `certificate_bindings` → `service_account_grants` chain.
    /// `None` = no binding / revoked / stale review / no scopes (fail closed).
    pub certificate_principal: Option<VerifiedPrincipal>,
    /// The binding/grant store could not be consulted. This is distinct from a
    /// clean `Ok(None)` miss: consumers must reject credential composition when
    /// server-controlled certificate state is unavailable.
    pub certificate_resolution_failed: bool,
    /// The certificate binding/grant store could not be consulted because of a
    /// transient OUTAGE — as opposed to deps-absent/unconfigured, which stays
    /// `certificate_resolution_failed`. Routed to a retryable `Unavailable` so a
    /// DB blip during mTLS resolution is not reported as a bad certificate
    /// (UDB-DB-READINESS-001).
    pub certificate_resolution_unavailable: bool,
    /// Canonical revalidation handle for long-lived served calls. This is
    /// populated only by the real Tower credential layer; direct handler tests
    /// that bypass the layer do not accidentally simulate revocation safety.
    pub revalidator: Option<CredentialRevalidator>,
}

/// Durable-store handles for async credential resolution. Installed once at
/// startup (both the full serve path and the DataBroker-only early path).
pub struct AuthPlaneDeps {
    pub pool: sqlx::PgPool,
    pub api_key_store: Arc<PostgresApiKeyStore>,
    pub api_key_hash_key: Vec<u8>,
    /// The same fully wired authn adapter used by native token validation. CDC
    /// revalidation therefore observes durable user/session/JTI/grant state and
    /// the configured Postgres/Redis session backend without parallel logic.
    pub bearer_state_validator:
        Arc<crate::runtime::service::auth_service::AuthnServiceImpl>,
}

static AUTH_PLANE_DEPS: OnceLock<RwLock<Option<Arc<AuthPlaneDeps>>>> = OnceLock::new();

fn auth_plane_deps_cell() -> &'static RwLock<Option<Arc<AuthPlaneDeps>>> {
    AUTH_PLANE_DEPS.get_or_init(|| RwLock::new(None))
}

/// Install the shared auth-plane dependencies. Idempotent; later installs
/// replace earlier ones (config reload).
pub fn install_auth_plane_deps(deps: Arc<AuthPlaneDeps>) {
    if let Ok(mut guard) = auth_plane_deps_cell().write() {
        *guard = Some(deps);
    }
}

fn auth_plane_deps() -> Option<Arc<AuthPlaneDeps>> {
    auth_plane_deps_cell()
        .read()
        .ok()
        .and_then(|guard| guard.as_ref().map(Arc::clone))
}

/// A8 (UDB-DB-READINESS-001): is the auth-plane PostgreSQL reachable right now?
/// A short-timeout `SELECT 1` on the shared pool. Returns `None` when the
/// auth-plane deps are not installed on this listener — the caller MUST NOT treat
/// that as an outage. Used by the per-listener live readiness-refresh task to flip
/// the gRPC serving status when the database is lost at runtime (the boot gate
/// only covers startup).
pub(crate) async fn auth_plane_pg_reachable() -> Option<bool> {
    let deps = auth_plane_deps()?;
    Some(pg_pool_reachable(&deps.pool).await)
}

/// Short-timeout `SELECT 1` reachability probe for an arbitrary pool. Used by the
/// live readiness-refresh task so each listener probes the pool its own services
/// actually use — the DataBroker plane probes the data-plane pool, not the
/// auth-plane pool (R-6).
pub(crate) async fn pg_pool_reachable(pool: &sqlx::PgPool) -> bool {
    matches!(
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            sqlx::query("SELECT 1").execute(pool),
        )
        .await,
        Ok(Ok(_))
    )
}

fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

fn header_value(headers: &http::HeaderMap, name: &str) -> String {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .trim()
        .to_string()
}

/// The first verified peer-certificate DER from the connection info tonic
/// stamps into request extensions on TLS listeners.
fn first_peer_cert_der(extensions: &http::Extensions) -> Option<Vec<u8>> {
    use tonic::transport::server::{TcpConnectInfo, TlsConnectInfo};
    extensions
        .get::<TlsConnectInfo<TcpConnectInfo>>()
        .and_then(|info| info.peer_certs())
        .as_deref()
        .and_then(|certs| certs.first())
        .map(|cert| cert.as_ref().to_vec())
}

/// Resolve every async credential present on the request. Never denies —
/// annotation only; failures surface as fail-closed outcomes for the sync
/// enforcement layer.
async fn resolve_credentials(
    headers: &http::HeaderMap,
    peer_cert_der: Option<Vec<u8>>,
) -> Option<PreresolvedCredentials> {
    let bearer = headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().strip_prefix("Bearer "))
        .map(str::to_string);
    let api_key_value = {
        let primary = header_value(headers, "x-api-key");
        if primary.is_empty() {
            header_value(headers, "x-udb-api-key")
        } else {
            primary
        }
    };
    let api_key_value = (!api_key_value.is_empty()).then_some(api_key_value);
    let api_key = api_key_value
        .clone()
        .and_then(|key| auth_plane_deps().map(|deps| (deps, key)));
    let has_cert = peer_cert_der.is_some();
    if bearer.is_none() && api_key_value.is_none() && !has_cert {
        return None;
    }

    let mut resolved = PreresolvedCredentials::default();
    resolved.revalidator = Some(CredentialRevalidator {
        bearer: bearer.clone(),
        api_key: api_key_value,
        peer_cert_der: peer_cert_der.clone(),
    });

    if let Some(token) = bearer {
        let outcome = match crate::runtime::security::validate_bearer_token_cached(
            &crate::runtime::security::SecurityConfig::current(),
            &token,
        ) {
            Ok(claims) => {
                let principal = VerifiedPrincipal::from_verified_bearer_claims(&claims);
                match auth_plane_deps() {
                    Some(deps) => match deps
                        .bearer_state_validator
                        .jwt_persisted_state_valid(&claims, now_unix())
                        .await
                    {
                        Ok(true) => Ok(principal),
                        Ok(false) => Err(
                            "bearer credential is revoked, expired, inactive, or no longer granted"
                                .to_string(),
                        ),
                        Err(status) => {
                            // Durable validation reports adapter/store failures as
                            // Internal in a few legacy seams. They are still an
                            // inability to decide, not proof that the credential
                            // is bad: preserve fail-closed behavior but return a
                            // retryable dependency status to the served client.
                            let status = if matches!(
                                status.code(),
                                tonic::Code::Internal | tonic::Code::Unavailable
                            ) {
                                crate::runtime::executor_utils::retryable_status(
                                    "authn",
                                    "bearer_durable_state_validate",
                                    crate::runtime::executor_utils::HTTP_RETRYABLE_BACKOFF_MS,
                                    "bearer credential state could not be validated because the credential store is unavailable",
                                )
                            } else {
                                status
                            };
                            Err(crate::runtime::executor_utils::store_string_from_status(
                                "bearer durable-state validation",
                                &status,
                            ))
                        }
                    },
                    None => Err(
                        "bearer validation requires the durable auth-state resolver".to_string(),
                    ),
                }
            }
            Err(error) => Err(error),
        };
        resolved.bearer = Some(outcome);
    }

    if let Some((deps, key)) = api_key {
        let outcome = match crate::runtime::authn::validate_api_key(
            deps.api_key_store.as_ref(),
            &key,
            &deps.api_key_hash_key,
            now_unix(),
        )
        .await
        {
            Ok(Some(record)) => {
                // CRIT-4: the key's stored scopes are attenuated against the
                // owner's CURRENT typed grant — a revoked/replaced grant or a
                // deactivated owner kills the key at authentication time, and
                // the effective scopes can never exceed the live grant.
                match crate::runtime::service::auth_service::grants::attenuate_key_scopes_against_grant(
                    &deps.pool,
                    &record.tenant_id,
                    &record.principal_id,
                    record.grant_revision,
                    &record.scopes,
                )
                .await
                {
                    Ok(Some(effective_scopes)) => Ok(Some(VerifiedPrincipal {
                        credential_type: 3, // CREDENTIAL_TYPE_API_KEY
                        subject: record.principal_id,
                        service_identity: record.service_identity,
                        tenant_id: record.tenant_id,
                        project_id: record.project_id,
                        scopes: effective_scopes,
                        roles: Vec::new(),
                        credential_id: record.key_prefix,
                        auth_method: "api_key".to_string(),
                        expires_at_unix: i64::try_from(record.expires_at_unix)
                            .unwrap_or(i64::MAX),
                        certificate_identity: None,
                        // The key's own per-minute budget (0 when the column is
                        // unset) — the opt-in limiter uses it to raise this key.
                        rate_limit_per_minute: record.rate_limit_per_minute,
                    })),
                    // Grant revoked / owner inactive / empty intersection —
                    // the key no longer authenticates (fail closed).
                    Ok(None) => Ok(None),
                    Err(error) => Err(error),
                }
            }
            Ok(None) => Ok(None),
            Err(error) => Err(error),
        };
        resolved.api_key = Some(outcome);
    }

    if let Some(der) = peer_cert_der {
        resolved.certificate_present = true;
        resolved.certificate_identity = crate::runtime::security::service_identity_from_der(&der);
        // HIGH-1: the binding resolves whenever a verified certificate is
        // present — even alongside a bearer or api key — so the sync layer can
        // COMPOSE the certificate's server-controlled binding (tenant/project/
        // service identity) against the other credential and reject splices.
        {
            if let Some(deps) = auth_plane_deps() {
                match crate::runtime::service::auth_service::grants::resolve_certificate_grant(
                    &deps.pool, &der,
                )
                .await
                {
                    Ok(Some(grant)) => {
                        resolved.certificate_principal = Some(VerifiedPrincipal {
                            credential_type: 5, // CREDENTIAL_TYPE_MTLS
                            subject: grant.subject,
                            service_identity: grant.service_identity,
                            tenant_id: grant.tenant_id,
                            project_id: grant.project_id,
                            scopes: grant.scopes,
                            roles: Vec::new(),
                            credential_id: grant.credential_id,
                            auth_method: "mtls".to_string(),
                            // Certificate validity and grant state are checked
                            // when the principal is resolved. Long-lived calls
                            // force bounded re-resolution independently.
                            expires_at_unix: 0,
                            certificate_identity: resolved.certificate_identity.clone(),
                            // mTLS grants carry no api-key budget — tenant default.
                            rate_limit_per_minute: 0,
                        });
                    }
                    Ok(None) => {
                        // A verified certificate was presented but has no
                        // currently valid server-controlled binding. Preserve
                        // that as a fail-closed credential outcome instead of
                        // silently ignoring the certificate during composition.
                    }
                    Err(error) => {
                        // A transient store OUTAGE (tagged Unavailable by the
                        // resolver) is retryable, not a bad certificate; any
                        // non-transient failure stays a terminal fail-closed
                        // denial. See UDB-DB-READINESS-001.
                        if crate::runtime::executor_utils::status_from_store_string(error.clone())
                            .code()
                            == tonic::Code::Unavailable
                        {
                            resolved.certificate_resolution_unavailable = true;
                        } else {
                            resolved.certificate_resolution_failed = true;
                        }
                        tracing::warn!(error = %error,
                            "certificate binding resolution failed (fail closed)");
                    }
                }
            } else {
                resolved.certificate_resolution_failed = true;
            }
        }
    }

    Some(resolved)
}

impl CredentialRevalidator {
    fn bearer_resolution_status(operation: &str, reason: String) -> tonic::Status {
        let typed = crate::runtime::executor_utils::status_from_store_string(reason);
        if typed.code() == tonic::Code::Unavailable {
            typed
        } else {
            crate::runtime::executor_utils::unauthenticated_status(
                operation,
                "CDC subscription credential is no longer valid; authenticate again",
            )
        }
    }

    fn api_key_resolution_status() -> tonic::Status {
        // The request-time API-key path treats resolver errors as dependency
        // outages: unknown/revoked keys are `Ok(None)`, while `Err` means the
        // durable authority could not make a decision. Preserve that contract
        // during stream revalidation instead of misreporting an outage as a bad
        // credential.
        crate::runtime::executor_utils::retryable_status(
            "authn",
            "cdc_api_key_revalidate",
            crate::runtime::executor_utils::HTTP_RETRYABLE_BACKOFF_MS,
            "CDC API key could not be revalidated because the credential store is unavailable",
        )
    }

    fn effective_service_identity(principal: &VerifiedPrincipal) -> String {
        if !principal.service_identity.trim().is_empty() {
            principal.service_identity.clone()
        } else if principal.credential_type == 3 {
            // API-key requests use the stored owner as the stable service
            // identity when the key record has no explicit service identity.
            principal.subject.clone()
        } else {
            "unknown".to_string()
        }
    }

    fn same_scope_set(left: &[String], right: &[String]) -> bool {
        let normalize = |scopes: &[String]| {
            scopes
                .iter()
                .map(|scope| scope.trim().to_ascii_lowercase())
                .collect::<std::collections::BTreeSet<_>>()
        };
        normalize(left) == normalize(right)
    }

    /// Re-run the same signature + durable credential resolution used by the
    /// Tower request layer, then bind the result to the stream's original
    /// principal lineage. Scope/rate-limit changes are refreshed; identity,
    /// tenant, project, and credential lineage may never change underneath an
    /// already-open stream.
    pub(crate) async fn revalidate_security_context(
        &self,
        expected: &crate::runtime::security::SecurityContext,
    ) -> Result<crate::runtime::security::SecurityContext, tonic::Status> {
        let mut headers = http::HeaderMap::new();
        if let Some(token) = self.bearer.as_ref() {
            let value = http::HeaderValue::from_str(&format!("Bearer {token}")).map_err(|_| {
                crate::runtime::executor_utils::unauthenticated_status(
                    "cdc_bearer_evidence_invalid",
                    "CDC bearer revalidation evidence is invalid; authenticate again",
                )
            })?;
            headers.insert(http::header::AUTHORIZATION, value);
        }
        if let Some(key) = self.api_key.as_ref() {
            let value = http::HeaderValue::from_str(key).map_err(|_| {
                crate::runtime::executor_utils::unauthenticated_status(
                    "cdc_api_key_evidence_invalid",
                    "CDC API-key revalidation evidence is invalid; authenticate again",
                )
            })?;
            headers.insert("x-api-key", value);
        }

        let mut resolved = resolve_credentials(&headers, self.peer_cert_der.clone())
            .await
            .ok_or_else(|| {
                crate::runtime::executor_utils::unauthenticated_status(
                    "cdc_credential_evidence_missing",
                    "CDC subscription has no revalidatable credential; authenticate again",
                )
            })?;

        if resolved.certificate_resolution_unavailable {
            return Err(crate::runtime::executor_utils::retryable_status(
                "authn",
                "cdc_certificate_revalidate",
                crate::runtime::executor_utils::HTTP_RETRYABLE_BACKOFF_MS,
                "CDC certificate binding could not be revalidated because the credential store is unavailable",
            ));
        }
        if resolved.certificate_resolution_failed
            || (resolved.certificate_present && resolved.certificate_principal.is_none())
        {
            return Err(crate::runtime::executor_utils::unauthenticated_status(
                "cdc_certificate_binding_revoked",
                "CDC certificate binding is no longer valid; authenticate again",
            ));
        }

        let principal = if self.bearer.is_some() {
            match resolved.bearer.take() {
                Some(Ok(principal)) => principal,
                Some(Err(reason)) => {
                    return Err(Self::bearer_resolution_status(
                        "cdc_bearer_revalidate",
                        reason,
                    ));
                }
                None => {
                    return Err(crate::runtime::executor_utils::unauthenticated_status(
                        "cdc_bearer_revalidation_missing",
                        "CDC bearer could not be revalidated; authenticate again",
                    ));
                }
            }
        } else if self.api_key.is_some() {
            match resolved.api_key.take() {
                Some(Ok(Some(principal))) => principal,
                Some(Ok(None)) => {
                    return Err(crate::runtime::executor_utils::unauthenticated_status(
                        "cdc_api_key_revoked",
                        "CDC API key is revoked, expired, or no longer granted; authenticate again",
                    ));
                }
                Some(Err(_reason)) => {
                    return Err(Self::api_key_resolution_status());
                }
                None => {
                    return Err(crate::runtime::executor_utils::unauthenticated_status(
                        "cdc_api_key_revalidation_missing",
                        "CDC API key could not be revalidated; authenticate again",
                    ));
                }
            }
        } else {
            resolved.certificate_principal.clone().ok_or_else(|| {
                crate::runtime::executor_utils::unauthenticated_status(
                    "cdc_certificate_revalidation_missing",
                    "CDC client certificate could not be revalidated; authenticate again",
                )
            })?
        };

        if let Some(binding) = resolved.certificate_principal.as_ref()
            && (self.bearer.is_some() || self.api_key.is_some())
        {
            crate::runtime::security::enforce_certificate_credential_composition(
                binding,
                &principal.service_identity,
                &principal.tenant_id,
                &principal.project_id,
            )?;
        }

        let service_identity = Self::effective_service_identity(&principal);
        let lineage_matches = principal.credential_type == expected.credential_type
            && principal.credential_id == expected.credential_id
            && principal.subject == expected.user_id
            && principal.tenant_id == expected.tenant_id
            && principal.project_id == expected.project_id
            && principal.auth_method == expected.auth_method
            && service_identity == expected.service_identity;
        if !lineage_matches {
            return Err(crate::runtime::executor_utils::unauthenticated_status(
                "cdc_credential_lineage_changed",
                "CDC credential lineage changed during revalidation; authenticate again",
            ));
        }
        // `CdcEngine::stream_cdc` captures the subscriber's scope-derived
        // privilege class for its inner tenant/project topic filter. Continuing
        // after any scope-set change would let that captured classification
        // drift from the refreshed outer Casbin decision. Close the stream and
        // require a fresh admission so both layers derive from one scope set.
        if !Self::same_scope_set(&principal.scopes, &expected.scopes) {
            return Err(crate::runtime::executor_utils::policy_status_with_code(
                tonic::Code::PermissionDenied,
                "cdc_credential_revalidate",
                "cdc_credential_scopes_changed",
                "CDC credential scopes changed; reconnect to establish a new authorization context",
            ));
        }

        let mut refreshed = expected.clone();
        refreshed.scopes = principal.scopes;
        refreshed.expires_at_unix = principal.expires_at_unix;
        refreshed.rate_limit_per_minute = principal.rate_limit_per_minute;
        Ok(refreshed)
    }
}

/// Tower layer running the async credential resolution before dispatch.
/// Mirrors [`crate::runtime::otel::TraceExtractLayer`]'s shape so it mounts on
/// the same shared `ServiceBuilder` for every listener.
#[derive(Clone, Default)]
pub struct CredentialResolveLayer;

impl CredentialResolveLayer {
    pub fn new() -> Self {
        Self
    }
}

impl<S> tower::Layer<S> for CredentialResolveLayer {
    type Service = CredentialResolveService<S>;
    fn layer(&self, inner: S) -> Self::Service {
        CredentialResolveService { inner }
    }
}

#[derive(Clone)]
pub struct CredentialResolveService<S> {
    inner: S,
}

impl<S> tonic::server::NamedService for CredentialResolveService<S>
where
    S: tonic::server::NamedService,
{
    const NAME: &'static str = S::NAME;
}

impl<S, ReqBody> Service<http::Request<ReqBody>> for CredentialResolveService<S>
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

    fn call(&mut self, mut req: http::Request<ReqBody>) -> Self::Future {
        // Clone-swap so the boxed future owns a ready service (standard tower
        // idiom — the original must be left in its polled-ready state).
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        Box::pin(async move {
            let needs_resolution = req.headers().contains_key("x-api-key")
                || req.headers().contains_key("x-udb-api-key")
                || req
                    .headers()
                    .get("authorization")
                    .and_then(|value| value.to_str().ok())
                    .is_some_and(|value| value.trim().starts_with("Bearer "))
                || req
                    .extensions()
                    .get::<tonic::transport::server::TlsConnectInfo<
                        tonic::transport::server::TcpConnectInfo,
                    >>()
                    .is_some();
            if needs_resolution {
                let der = first_peer_cert_der(req.extensions());
                if let Some(resolved) = resolve_credentials(req.headers(), der).await {
                    req.extensions_mut().insert(resolved);
                }
            }
            inner.call(req).await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::CredentialRevalidator;

    #[test]
    fn scope_set_comparison_is_order_insensitive_but_detects_authority_change() {
        let original = vec!["udb:cdc:read".to_string(), "UDB:READ".to_string()];
        let reordered = vec!["udb:read".to_string(), "udb:cdc:read".to_string()];
        let narrowed = vec!["udb:cdc:read".to_string()];

        assert!(CredentialRevalidator::same_scope_set(
            &original, &reordered
        ));
        assert!(!CredentialRevalidator::same_scope_set(
            &original, &narrowed
        ));
    }

    #[test]
    fn credential_revalidator_debug_never_exposes_secret_evidence() {
        let revalidator = CredentialRevalidator {
            bearer: Some("secret-bearer-token".to_string()),
            api_key: Some("secret-api-key".to_string()),
            peer_cert_der: Some(vec![0xde, 0xad, 0xbe, 0xef]),
        };

        let debug = format!("{revalidator:?}");
        assert!(debug.contains("bearer: true"));
        assert!(debug.contains("api_key: true"));
        assert!(debug.contains("peer_cert_der: true"));
        assert!(!debug.contains("secret-bearer-token"));
        assert!(!debug.contains("secret-api-key"));
        assert!(!debug.contains("deadbeef"));
    }
}
