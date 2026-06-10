//! `AuthnService` handler over the UDB-owned authn primitives (sessions, API
//! keys, hybrid external identities, native JWT).

use std::sync::Arc;

use sqlx::PgPool;
#[cfg(feature = "webauthn")]
use sqlx::Row;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::proto::udb::core::authn::entity::v1 as authn_entity_pb;
use crate::proto::udb::core::authn::services::v1 as authn_pb;
use authn_pb::authn_service_server::AuthnService;

use crate::runtime::authn::{
    self, ApiKeyStore, AuthnConfig, ExternalJwtProvider, ExternalProviderConfig, IdentityProvider,
    OtpRecord, SessionRecord, SessionStore, UnavailableApiKeyStore, UnavailableSessionStore,
    UnavailableUserStore, UserRecord, UserStore,
};
use crate::runtime::authz::Principal;
#[cfg(feature = "webauthn")]
use crate::runtime::native_catalog::{NativeModel, native_model};
use crate::runtime::security::{SecurityConfig, validate_bearer_token};

use super::events::{self, AuthEvent, AuthEventSink, ComplianceEnvelope, topics};
use super::mappings::{
    authn_principal_to_pb, bounded_page_response, bounded_page_window, principal_from_api_key,
    principal_from_session, public_session_handle_from_hash, session_record_to_pb,
    timestamp_from_unix,
};
use super::now_unix;

// ── Proto ⟷ domain enum conversion (adapter boundary) ───────────────────────
// The domain engine (`runtime/authn`) stores domain-owned `AccountKind` /
// `AccountStatus` enums and never references the proto enums (final_task.md §7
// "Move proto enum conversions out of domain engines"). These helpers convert at
// the service boundary, where the proto types legitimately live. The integer
// values coincide with the proto enum tags, so the conversion is a checked cast.
use crate::runtime::authn::{AccountKind, AccountStatus};

/// Proto `AccountKind` i32 → domain [`AccountKind`].
pub(super) fn account_kind_from_proto(value: i32) -> AccountKind {
    AccountKind::from_i32(value)
}

/// Domain [`AccountKind`] → proto `AccountKind` i32.
pub(super) fn account_kind_to_proto(kind: AccountKind) -> i32 {
    // Defined via the proto enum so a proto-side renumbering is caught here, not
    // silently mismatched: map the domain variant to its proto counterpart.
    let proto = match kind {
        AccountKind::Unspecified => authn_entity_pb::AccountKind::Unspecified,
        AccountKind::Person => authn_entity_pb::AccountKind::Person,
        AccountKind::ServiceAccount => authn_entity_pb::AccountKind::ServiceAccount,
        AccountKind::Workload => authn_entity_pb::AccountKind::Workload,
        AccountKind::ExternalIdentity => authn_entity_pb::AccountKind::ExternalIdentity,
        AccountKind::System => authn_entity_pb::AccountKind::System,
        AccountKind::Anonymous => authn_entity_pb::AccountKind::Anonymous,
    };
    proto as i32
}

/// Proto `UserStatus` i32 → domain [`AccountStatus`].
pub(super) fn account_status_from_proto(value: i32) -> AccountStatus {
    AccountStatus::from_i32(value)
}

/// Domain [`AccountStatus`] → proto `UserStatus` i32.
pub(super) fn account_status_to_proto(status: AccountStatus) -> i32 {
    let proto = match status {
        AccountStatus::Unspecified => authn_entity_pb::UserStatus::Unspecified,
        AccountStatus::PendingVerification => authn_entity_pb::UserStatus::PendingVerification,
        AccountStatus::Active => authn_entity_pb::UserStatus::Active,
        AccountStatus::Suspended => authn_entity_pb::UserStatus::Suspended,
        AccountStatus::Locked => authn_entity_pb::UserStatus::Locked,
        AccountStatus::Deactivated => authn_entity_pb::UserStatus::Deactivated,
    };
    proto as i32
}

// Topic submodules: each holds inherent `impl AuthnServiceImpl` blocks + helpers;
// the single `impl AuthnService` trait block below delegates to them.
mod core;
mod lifecycle;
mod login;
mod mfa;
mod sessions;
mod signing_keys;
mod token_family;
mod tokens;

/// `AuthnService` handler over the UDB-owned authn primitives.
pub struct AuthnServiceImpl {
    sessions: Arc<dyn SessionStore>,
    api_keys: Arc<dyn ApiKeyStore>,
    users: Arc<dyn UserStore>,
    config: AuthnConfig,
    security: SecurityConfig,
    pg_pool: Option<PgPool>,
    /// Publishes authn domain events to the outbox → Kafka relay.
    event_sink: Arc<dyn AuthEventSink>,
    /// Records auth-plane metrics (login success/failure, lockout, MFA failure,
    /// token-validation failure). Defaults to a no-op; production wires the
    /// broker's shared recorder via [`AuthnServiceImpl::with_metrics`].
    metrics: Arc<dyn crate::metrics::MetricsRecorder>,
    /// Short-TTL cluster jti denylist (Redis) — an acceleration layer over the
    /// durable `token_revocations` table so a revoke propagates fast across
    /// nodes. `None` when Redis isn't configured (DB-only behavior unchanged).
    #[cfg(feature = "redis")]
    jti_denylist: Option<crate::runtime::authn::revocation::JtiDenylist>,
    /// The shared, atomically-swappable authz snapshot — the SAME view the native
    /// `AuthzService` decides against. Wired so the admin-mutation handlers can
    /// invoke the native authz DECISION ENGINE per action (Tier-0 #5, D2-full),
    /// recording a real `decision_id` beyond the coarse transport scope gate.
    /// `None` keeps the scope-only defense-in-depth path working (no engine wired).
    authz_snapshot: Option<std::sync::Arc<arc_swap::ArcSwap<crate::runtime::authz::AuthzSnapshot>>>,
}

#[cfg(feature = "webauthn")]
struct WebAuthnConfig {
    rp_id: String,
    rp_origin: String,
    rp_name: String,
    challenge_ttl_secs: u64,
}

#[cfg(feature = "webauthn")]
struct WebAuthnChallengeRecord {
    user_id: String,
    state_json: String,
    tenant_id: String,
    project_id: String,
}

#[cfg(feature = "webauthn")]
struct WebAuthnPasskeyRecord {
    passkey_json: String,
}

#[cfg(feature = "webauthn")]
#[derive(serde::Serialize, serde::Deserialize)]
struct WebAuthnStateEnvelope<T> {
    state: T,
    label: String,
}

#[cfg(feature = "webauthn")]
fn webauthn_credential_model() -> NativeModel {
    native_model(
        "udb.core.authn.entity.v1.WebAuthnCredential",
        &[
            "credential_id",
            "user_id",
            "passkey_json",
            "label",
            "tenant_id",
            "project_id",
            "created_at",
            "updated_at",
            "last_used_at",
        ],
    )
}

#[cfg(feature = "webauthn")]
fn webauthn_challenge_model() -> NativeModel {
    native_model(
        "udb.core.authn.entity.v1.WebAuthnChallenge",
        &[
            "challenge_id",
            "user_id",
            "ceremony",
            "state_json",
            "tenant_id",
            "project_id",
            "expires_at",
            "consumed_at",
            "created_at",
        ],
    )
}

#[cfg(feature = "webauthn")]
fn principal_from_user_record(user: &UserRecord, method: authn::AuthnMethod) -> Principal {
    Principal {
        principal_id: user.user_id.clone(),
        subject: user.user_id.clone(),
        user_id: user.user_id.clone(),
        service_identity: String::new(),
        tenant_id: user.tenant_id.clone(),
        project_id: user.project_id.clone(),
        scopes: Vec::new(),
        roles: Vec::new(),
        provider_id: String::new(),
        auth_method: method.as_str().to_string(),
    }
}

/// Test-only capture of issued OTP plaintext codes, keyed by `otp_id`. Because
/// codes are now CSPRNG-drawn (and only the hash is stored), tests cannot
/// recompute them; this lets a test look up the code it would have received by
/// email. Keyed by `otp_id` so parallel tests don't clobber each other.
#[cfg(test)]
pub(crate) fn test_otp_codes()
-> &'static std::sync::Mutex<std::collections::HashMap<String, String>> {
    static CODES: std::sync::OnceLock<std::sync::Mutex<std::collections::HashMap<String, String>>> =
        std::sync::OnceLock::new();
    CODES.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

impl AuthnServiceImpl {
    pub fn new(config: AuthnConfig, security: SecurityConfig) -> Self {
        Self {
            sessions: Arc::new(UnavailableSessionStore),
            api_keys: Arc::new(UnavailableApiKeyStore),
            users: Arc::new(UnavailableUserStore),
            config,
            security,
            pg_pool: None,
            event_sink: events::noop_sink(),
            metrics: Arc::new(crate::metrics::NoopMetrics),
            #[cfg(feature = "redis")]
            jti_denylist: None,
            authz_snapshot: None,
        }
    }

    /// Construct with explicit stores (used by tests / DB-backed wiring).
    pub fn with_stores(
        config: AuthnConfig,
        security: SecurityConfig,
        sessions: Arc<dyn SessionStore>,
        api_keys: Arc<dyn ApiKeyStore>,
        users: Arc<dyn UserStore>,
    ) -> Self {
        Self {
            sessions,
            api_keys,
            users,
            config,
            security,
            pg_pool: None,
            event_sink: events::noop_sink(),
            metrics: Arc::new(crate::metrics::NoopMetrics),
            #[cfg(feature = "redis")]
            jti_denylist: None,
            authz_snapshot: None,
        }
    }

    pub fn with_postgres(mut self, pool: Option<PgPool>) -> Self {
        self.pg_pool = pool;
        self
    }

    /// Attach the short-TTL cluster jti denylist (Redis). When set, revokes also
    /// SET the jti_hash in Redis and validation consults it first; when `None`
    /// the durable DB lookup is the only path (behavior unchanged).
    #[cfg(feature = "redis")]
    pub(crate) fn with_jti_denylist(
        mut self,
        denylist: Option<crate::runtime::authn::revocation::JtiDenylist>,
    ) -> Self {
        self.jti_denylist = denylist;
        self
    }

    /// Attach the domain-event sink (outbox → Kafka). Defaults to a no-op.
    pub(crate) fn with_event_sink(mut self, sink: Arc<dyn AuthEventSink>) -> Self {
        self.event_sink = sink;
        self
    }

    /// Attach the metrics recorder (the broker's shared recorder in production).
    /// Defaults to a no-op.
    pub(crate) fn with_metrics(
        mut self,
        metrics: Arc<dyn crate::metrics::MetricsRecorder>,
    ) -> Self {
        self.metrics = metrics;
        self
    }

    /// Wire the shared authz snapshot (the SAME `Arc<ArcSwap<AuthzSnapshot>>` the
    /// native `AuthzService` decides against) so the admin-mutation handlers can
    /// invoke the native authz DECISION ENGINE per action (Tier-0 #5, D2-full).
    /// When unset the handlers fall back to the coarse scope gate + action-scope
    /// check only (defense-in-depth path, behavior unchanged).
    pub(crate) fn with_authz_snapshot(
        mut self,
        snapshot: Option<std::sync::Arc<arc_swap::ArcSwap<crate::runtime::authz::AuthzSnapshot>>>,
    ) -> Self {
        self.authz_snapshot = snapshot;
        self
    }

    /// Per-action native authz DECISION (Tier-0 #5, D2-full). Beyond the coarse
    /// transport `udb:admin` gate AND the action-scope check, this asks the native
    /// authz DECISION ENGINE (the shared `AuthzSnapshot`, same view `AuthzService`
    /// decides against) whether `(principal, action, resource)` is permitted, and
    /// returns the engine's real `decision_id` on allow (to thread into the audit
    /// compliance envelope). DENIES with the engine's reason on a negative
    /// decision.
    ///
    /// `grpc_path` is the RPC's gRPC path; the resource is the method's
    /// `endpoint_security.decision_resource` (when annotated) else the synthetic
    /// `native.rpc:<path>` form. When no snapshot is wired, or no over-the-wire
    /// claim context is installed (in-process/test caller), this is a no-op that
    /// returns a fresh decision id — the same posture as `authorize_action`, so it
    /// never weakens the existing scope-gate path nor breaks direct invocation.
    pub(super) async fn decide_action_native(
        &self,
        ctx: &crate::runtime::service::method_security::VerifiedClaimContext,
        grpc_path: &str,
        action: &str,
    ) -> Result<String, Status> {
        use crate::runtime::authz::{Authorizer, AuthzQuery, ResourceRef};

        // No engine wired, or not an over-the-wire request → skip (the action-scope
        // gate already ran). Return a fresh id so the envelope still links a value.
        let Some(snapshot) = self.authz_snapshot.as_ref() else {
            return Ok(uuid::Uuid::new_v4().to_string());
        };
        if !crate::runtime::service::method_security::claim_context_present() {
            return Ok(uuid::Uuid::new_v4().to_string());
        }

        let principal = ctx.to_principal();
        let resource_name =
            crate::runtime::service::method_security::decision_resource_for(grpc_path);
        let resource = ResourceRef {
            resource_type: "native.rpc".to_string(),
            resource_name: resource_name.clone(),
            ..ResourceRef::default()
        };
        // The method's declared `endpoint_security.policy_ref` names the policy set
        // this action must be evaluated against (Tier-0 #5 / D2-full). Surface it as
        // a request attribute so a policy scoped to that named set (via a
        // `policy_ref` condition) matches — letting an operator govern a specific
        // native RPC's authorization by name, not only by resource pattern. Absent
        // when the annotation left it unset.
        let mut attributes = std::collections::BTreeMap::new();
        if let Some(policy_ref) =
            crate::runtime::service::method_security::method_policy_ref(grpc_path)
        {
            attributes.insert("policy_ref".to_string(), policy_ref);
        }
        let query = AuthzQuery {
            principal: &principal,
            resource: &resource,
            action,
            purpose: "",
            attributes: &attributes,
        };
        // Synchronous v2 snapshot decision (policy/role/relationship match) — the
        // same engine the AuthzService `check_access` path uses; no Casbin model
        // round-trip needed for the structured snapshot decision.
        let decision = snapshot.load().authorize(&query);
        if decision.allowed {
            return Ok(decision.decision_id);
        }
        // Distinguish an EXPLICIT deny (a policy matched this resource/action and
        // denied) from a "no policy governs this native RPC yet" default-deny. The
        // action-scope gate (`authorize_action`) already authorized the caller; an
        // engine with no rule for `native.rpc:*` resources must NOT retroactively
        // deny every legitimate operator (that would be a capability lie / outage
        // for ungoverned deployments). So we only DENY on an EXPLICIT matched deny;
        // a bare default-deny falls through, still recording the engine decision_id
        // for the audit envelope (the action-scope authorization stands).
        if decision.matched_policy_ids.is_empty() {
            tracing::debug!(
                target: "udb.audit.authz",
                subject = %ctx.subject,
                action = %action,
                resource = %resource_name,
                policy_ref = attributes.get("policy_ref").map(String::as_str).unwrap_or(""),
                decision_id = %decision.decision_id,
                "no native-RPC authz policy governs this action; action-scope authorization stands"
            );
            return Ok(decision.decision_id);
        }
        tracing::warn!(
            target: "udb.audit.authz",
            subject = %ctx.subject,
            tenant = %ctx.tenant_id,
            action = %action,
            resource = %resource_name,
            policy_ref = attributes.get("policy_ref").map(String::as_str).unwrap_or(""),
            decision_id = %decision.decision_id,
            reason = %decision.deny_reason,
            "DENY: native authz decision engine denied the admin mutation"
        );
        Err(Status::permission_denied(format!(
            "action '{action}' on '{resource_name}' denied by authz policy ({})",
            decision.deny_reason
        )))
    }

    /// Emit an authn domain event, logging (never failing the RPC) on error.
    // Shared helpers used across the topic submodules — `pub(super)` so each
    // `impl AuthnServiceImpl` block in a sibling module can call them.
    pub(super) async fn emit_event(&self, event: AuthEvent) {
        let topic = event.topic;
        if let Err(err) = self.event_sink.emit(event).await {
            tracing::warn!(topic, error = %err, "failed to publish authn event");
        }
    }

    /// Crate-public best-effort emit for the operations plane (`udb.ops.*`),
    /// used by the serving startup path to publish boot-time operational events
    /// (e.g. `OPS_READINESS_FAILURE`) through the SAME sink as every other auth
    /// event. `emit_event` is `pub(super)` (sibling-module only); this exposes a
    /// reachable entry point to `runtime::service::serve` without widening the
    /// per-handler emit surface.
    pub(crate) async fn emit_ops_event(&self, event: AuthEvent) {
        self.emit_event(event).await;
    }

    /// Emit an authn domain event durably WITHIN the caller's transaction
    /// connection, so the audit/outbox row commits atomically with the mutation
    /// (final_task.md §7: "auth mutations either commit with their event or
    /// fail"). Unlike [`Self::emit_event`], a write failure is propagated so the
    /// caller can abort the transaction — the mutation and its event are all-or-
    /// nothing.
    pub(super) async fn emit_event_in_tx(
        &self,
        conn: &mut sqlx::PgConnection,
        event: AuthEvent,
    ) -> Result<(), Status> {
        let topic = event.topic;
        self.event_sink
            .write_in_tx(conn, event)
            .await
            .map_err(|err| Status::internal(format!("audit event write failed for {topic}: {err}")))
    }

    pub(super) fn hash_key(&self) -> Vec<u8> {
        self.config.session_hash_secret.as_bytes().to_vec()
    }

    pub(super) fn api_key_hash_key(&self) -> Vec<u8> {
        self.config.api_key_hash_secret().as_bytes().to_vec()
    }

    pub(super) fn password_hash_key(&self) -> Vec<u8> {
        self.config.password_hash_secret().as_bytes().to_vec()
    }

    pub(super) fn otp_hash_key(&self) -> Vec<u8> {
        self.config.otp_hash_secret().as_bytes().to_vec()
    }

    /// Derive the CSRF token bound to a session id (signed double-submit cookie).
    /// Issued at login and re-derived for validation; never stored.
    pub(super) fn csrf_token_for(&self, session_id: &str) -> String {
        authn::hash_secret(&format!("csrf:{session_id}"), &self.hash_key())
    }

    /// Phase 3: is a UDB-issued JWT still backed by live persisted auth state?
    /// Returns `Ok(false)` when the subject user is no longer Active, or when the
    /// issuing session (carried as the JWT `jti`, prefix `sess_`) has been
    /// revoked/expired — so a signature-valid, unexpired token stops being
    /// honored the moment the user is suspended or the session is revoked
    /// (revocation before natural expiry). Service-identity tokens and subjects
    /// that are neither a UDB user id nor a session id are exempt (validated by
    /// signature alone). Shared by `ValidateToken` and the `Authenticate` bearer
    /// path. `Err` is reserved for real backend failures (fail-closed at the
    /// call site).
    /// Is `user_id` a live, Active native UDB user? Only UUID subjects are
    /// status-gated; empty/non-UUID subjects (service identities, external
    /// subjects) have no user row and return `Ok(true)` (not gated here). `Err`
    /// is reserved for real backend failures.
    pub(super) async fn user_is_active(&self, user_id: &str) -> Result<bool, Status> {
        if user_id.trim().is_empty() || Uuid::parse_str(user_id).is_err() {
            return Ok(true);
        }
        match self.users.get_user_by_id(user_id).await {
            Ok(Some(user)) => Ok(user.status.is_active()),
            Ok(None) => Ok(false),
            Err(err) => Err(Status::internal(err)),
        }
    }

    pub(super) async fn jwt_persisted_state_valid(
        &self,
        claims: &crate::runtime::security::SecurityClaims,
        now: u64,
    ) -> Result<bool, Status> {
        let is_service = claims
            .service_identity
            .as_deref()
            .is_some_and(|s| !s.is_empty());
        if !is_service
            && !self
                .user_is_active(&claims.sub.clone().unwrap_or_default())
                .await?
        {
            return Ok(false);
        }
        // Phase 3 (I2.3): reject any token whose jti is on the durable cluster-wide
        // revocation deny list, before its natural expiry — across every node.
        if let Some(jti) = claims.jti.as_deref().filter(|j| !j.trim().is_empty())
            && self.is_token_revoked(jti).await.0
        {
            return Ok(false);
        }
        if let Some(jti) = claims.jti.as_deref().filter(|j| j.starts_with("sess_")) {
            match authn::validate_session(
                self.sessions.as_ref(),
                jti,
                &self.hash_key(),
                now,
                self.config.session_idle_ttl_secs,
            )
            .await
            {
                Ok(Some(_)) => {}
                Ok(None) => return Ok(false),
                Err(err) => return Err(Status::internal(err)),
            }
        }
        Ok(true)
    }

    #[cfg(feature = "webauthn")]
    fn require_pg_pool(&self) -> Result<&PgPool, Status> {
        self.pg_pool.as_ref().ok_or_else(|| {
            Status::failed_precondition(
                "WebAuthn requires the native Postgres auth store to persist passkeys and challenges",
            )
        })
    }

    #[cfg(feature = "oidc")]
    pub(super) async fn authenticate_oidc_token(
        &self,
        req: &authn_pb::AuthnRequest,
    ) -> Result<Principal, Status> {
        use openidconnect::core::{CoreClient, CoreIdToken, CoreProviderMetadata};
        use openidconnect::reqwest;
        use openidconnect::{ClientId, ClientSecret, IssuerUrl, Nonce};
        use std::str::FromStr;

        let id_token = if !req.external_token.trim().is_empty() {
            req.external_token.trim().to_string()
        } else {
            req.bearer_token.trim().to_string()
        };
        if id_token.is_empty() {
            return Err(Status::invalid_argument(
                "OIDC authentication requires external_token or bearer_token containing an ID token",
            ));
        }
        if id_token.matches('.').count() != 2 {
            return Err(Status::unauthenticated("invalid credential"));
        }
        // J2.1: resolve the provider from the per-tenant registry first, so OIDC
        // config is no longer a process global. The request/env values remain as
        // fallbacks (dev defaults) when no provider row matches. A provider that
        // is found but disabled blocks new logins (fail closed).
        let registered = super::idp::resolve_oidc_provider(
            self.pg_pool.as_ref(),
            &req.tenant_hint,
            &req.external_provider_id,
        )
        .await
        .unwrap_or(None);
        if let Some(p) = &registered {
            if !p.enabled {
                return Err(Status::failed_precondition(
                    "identity provider is disabled for this tenant",
                ));
            }
        }
        let registered_issuer = registered
            .as_ref()
            .map(|p| p.issuer.clone())
            .unwrap_or_default();
        let registered_client_id = registered
            .as_ref()
            .and_then(|p| p.client_ids.first().cloned())
            .unwrap_or_default();
        let issuer = if !req.issuer.trim().is_empty() {
            req.issuer.trim().to_string()
        } else if !registered_issuer.trim().is_empty() {
            registered_issuer.trim().to_string()
        } else {
            std::env::var("UDB_OIDC_ISSUER").unwrap_or_default()
        };
        if issuer.is_empty() {
            return Err(Status::invalid_argument(
                "OIDC issuer is required (provider registry, request issuer, or UDB_OIDC_ISSUER)",
            ));
        }
        let client_id = if !req.client_id.trim().is_empty() {
            req.client_id.trim().to_string()
        } else if !req.audience.trim().is_empty() {
            req.audience.trim().to_string()
        } else if !registered_client_id.trim().is_empty() {
            registered_client_id.trim().to_string()
        } else {
            std::env::var("UDB_OIDC_CLIENT_ID").unwrap_or_default()
        };
        if client_id.is_empty() {
            return Err(Status::invalid_argument(
                "OIDC client_id/audience is required",
            ));
        }
        if !req.client_id.trim().is_empty()
            && !req.audience.trim().is_empty()
            && req.client_id.trim() != req.audience.trim()
        {
            return Err(Status::invalid_argument(
                "OIDC client_id and audience must match",
            ));
        }
        let nonce = req
            .attributes
            .get("nonce")
            .cloned()
            .or_else(|| std::env::var("UDB_OIDC_NONCE").ok())
            .unwrap_or_default();
        if nonce.trim().is_empty() {
            return Err(Status::invalid_argument(
                "OIDC nonce is required in attributes[\"nonce\"]",
            ));
        }
        let provider_id = if !req.external_provider_id.trim().is_empty() {
            req.external_provider_id.trim().to_string()
        } else {
            "oidc".to_string()
        };
        let tenant_id = req.tenant_hint.clone();
        let project_id = req.project_hint.clone();
        let scopes = req.requested_scopes.clone();

        tokio::task::spawn_blocking(move || {
            let http_client = reqwest::blocking::ClientBuilder::new()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .map_err(|err| Status::internal(format!("OIDC HTTP client build failed: {err}")))?;
            let issuer_url = IssuerUrl::new(issuer)
                .map_err(|err| Status::invalid_argument(format!("invalid OIDC issuer: {err}")))?;
            let provider_metadata = CoreProviderMetadata::discover(&issuer_url, &http_client)
                .map_err(|_| Status::unauthenticated("invalid credential"))?;
            let client_secret = std::env::var("UDB_OIDC_CLIENT_SECRET")
                .ok()
                .filter(|secret| !secret.trim().is_empty())
                .map(ClientSecret::new);
            let client = CoreClient::from_provider_metadata(
                provider_metadata,
                ClientId::new(client_id),
                client_secret,
            );
            let id_token = CoreIdToken::from_str(&id_token)
                .map_err(|_| Status::unauthenticated("invalid credential"))?;
            let verifier = client.id_token_verifier();
            let claims = id_token
                .claims(&verifier, &Nonce::new(nonce))
                .map_err(|_| Status::unauthenticated("invalid credential"))?;
            let subject = claims.subject().as_str().to_string();
            Ok(Principal {
                principal_id: format!("{provider_id}:{subject}"),
                subject: subject.clone(),
                user_id: subject,
                service_identity: String::new(),
                tenant_id,
                project_id,
                scopes,
                roles: Vec::new(),
                provider_id,
                auth_method: "oidc".to_string(),
            })
        })
        .await
        .map_err(|err| Status::internal(format!("OIDC verification task failed: {err}")))?
    }

    #[cfg(feature = "webauthn")]
    fn webauthn_config(&self) -> Result<WebAuthnConfig, Status> {
        let rp_id = std::env::var("UDB_WEBAUTHN_RP_ID").unwrap_or_default();
        let rp_origin = std::env::var("UDB_WEBAUTHN_ORIGIN").unwrap_or_default();
        if rp_id.trim().is_empty() || rp_origin.trim().is_empty() {
            return Err(Status::failed_precondition(
                "WebAuthn requires UDB_WEBAUTHN_RP_ID and UDB_WEBAUTHN_ORIGIN",
            ));
        }
        Ok(WebAuthnConfig {
            rp_name: std::env::var("UDB_WEBAUTHN_RP_NAME")
                .ok()
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| rp_id.clone()),
            challenge_ttl_secs: std::env::var("UDB_WEBAUTHN_CHALLENGE_TTL_SECONDS")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .filter(|ttl| *ttl > 0)
                .unwrap_or(300),
            rp_id,
            rp_origin,
        })
    }

    #[cfg(feature = "webauthn")]
    fn webauthn(&self) -> Result<webauthn_rs::prelude::Webauthn, Status> {
        // TODO(host): attestation/RK-UV policy needs the webauthn/openssl feature (Tier-7 #32)
        // — configurable attestation conveyance, resident-key (discoverable
        // credential) requirement, and user-verification policy are host-blocked
        // on the openssl-backed webauthn build; only the 3 IdP event families are
        // shippable in this pass.
        use std::time::Duration;
        use webauthn_rs::prelude::{Url, WebauthnBuilder};

        let cfg = self.webauthn_config()?;
        let origin = Url::parse(&cfg.rp_origin).map_err(|err| {
            Status::failed_precondition(format!("invalid WebAuthn origin: {err}"))
        })?;
        WebauthnBuilder::new(&cfg.rp_id, &origin)
            .map_err(|err| {
                Status::failed_precondition(format!("invalid WebAuthn RP config: {err:?}"))
            })?
            .rp_name(&cfg.rp_name)
            .timeout(Duration::from_secs(cfg.challenge_ttl_secs))
            .build()
            .map_err(|err| Status::failed_precondition(format!("invalid WebAuthn config: {err:?}")))
    }

    #[cfg(feature = "webauthn")]
    async fn store_webauthn_challenge(
        &self,
        user: &UserRecord,
        ceremony: &str,
        state_json: String,
        expires_at: u64,
    ) -> Result<String, Status> {
        let pool = self.require_pg_pool()?;
        let model = webauthn_challenge_model();
        let rel = &model.relation;
        let challenge_id = Uuid::new_v4().to_string();
        sqlx::query(&format!(
            "INSERT INTO {rel} ({}, {}, {}, {}, {}, {}, {}) VALUES ($1::UUID, $2::UUID, $3, $4::JSONB, $5, $6, to_timestamp($7::DOUBLE PRECISION))",
            model.q("challenge_id"), model.q("user_id"), model.q("ceremony"),
            model.q("state_json"), model.q("tenant_id"), model.q("project_id"), model.q("expires_at"),
        ))
        .bind(&challenge_id)
        .bind(&user.user_id)
        .bind(ceremony)
        .bind(state_json)
        .bind(&user.tenant_id)
        .bind(&user.project_id)
        .bind(expires_at as f64)
        .execute(pool)
        .await
        .map_err(|err| Status::internal(format!("store WebAuthn challenge failed: {err}")))?;
        Ok(challenge_id)
    }

    #[cfg(feature = "webauthn")]
    async fn load_webauthn_challenge(
        &self,
        challenge_id: &str,
        ceremony: &str,
    ) -> Result<WebAuthnChallengeRecord, Status> {
        let pool = self.require_pg_pool()?;
        let model = webauthn_challenge_model();
        let rel = &model.relation;
        let row = sqlx::query(&format!(
            "SELECT {}, {}, {}, {} FROM {rel} WHERE {} = $1::UUID AND {} = $2 AND {} IS NULL AND {} > NOW()",
            model.text("user_id"),
            model.json_text_as("state_json", "state_json"),
            model.text_or_empty("tenant_id"),
            model.text_or_empty("project_id"),
            model.q("challenge_id"),
            model.q("ceremony"),
            model.q("consumed_at"),
            model.q("expires_at"),
        ))
        .bind(challenge_id)
        .bind(ceremony)
        .fetch_optional(pool)
        .await
        .map_err(|err| Status::internal(format!("load WebAuthn challenge failed: {err}")))?
        .ok_or_else(|| Status::permission_denied("invalid WebAuthn ceremony"))?;
        Ok(WebAuthnChallengeRecord {
            user_id: row.try_get("user_id").map_err(|err| {
                Status::internal(format!("decode WebAuthn challenge user_id failed: {err}"))
            })?,
            state_json: row.try_get("state_json").map_err(|err| {
                Status::internal(format!(
                    "decode WebAuthn challenge state_json failed: {err}"
                ))
            })?,
            tenant_id: row.try_get("tenant_id").map_err(|err| {
                Status::internal(format!("decode WebAuthn challenge tenant_id failed: {err}"))
            })?,
            project_id: row.try_get("project_id").map_err(|err| {
                Status::internal(format!(
                    "decode WebAuthn challenge project_id failed: {err}"
                ))
            })?,
        })
    }

    #[cfg(feature = "webauthn")]
    async fn consume_webauthn_challenge(&self, challenge_id: &str) -> Result<(), Status> {
        let pool = self.require_pg_pool()?;
        let model = webauthn_challenge_model();
        let rel = &model.relation;
        sqlx::query(&format!(
            "UPDATE {rel} SET {} = NOW() WHERE {} = $1::UUID AND {} IS NULL",
            model.q("consumed_at"),
            model.q("challenge_id"),
            model.q("consumed_at"),
        ))
        .bind(challenge_id)
        .execute(pool)
        .await
        .map_err(|err| Status::internal(format!("consume WebAuthn challenge failed: {err}")))?;
        Ok(())
    }

    #[cfg(feature = "webauthn")]
    async fn load_webauthn_passkeys(
        &self,
        user_id: &str,
    ) -> Result<Vec<WebAuthnPasskeyRecord>, Status> {
        let pool = self.require_pg_pool()?;
        let model = webauthn_credential_model();
        let rel = &model.relation;
        let rows = sqlx::query(&format!(
            "SELECT {} FROM {rel} WHERE {} = $1::UUID ORDER BY {} ASC",
            model.json_text_as("passkey_json", "passkey_json"),
            model.q("user_id"),
            model.q("created_at"),
        ))
        .bind(user_id)
        .fetch_all(pool)
        .await
        .map_err(|err| Status::internal(format!("load WebAuthn passkeys failed: {err}")))?;
        rows.into_iter()
            .map(|row| {
                Ok(WebAuthnPasskeyRecord {
                    passkey_json: row.try_get("passkey_json").map_err(|err| {
                        Status::internal(format!("decode WebAuthn passkey_json failed: {err}"))
                    })?,
                })
            })
            .collect()
    }

    #[cfg(feature = "webauthn")]
    fn webauthn_credential_id_text(
        id: &webauthn_rs::prelude::CredentialID,
    ) -> Result<String, Status> {
        let value = serde_json::to_value(id).map_err(|err| {
            Status::internal(format!("serialize WebAuthn credential id failed: {err}"))
        })?;
        Ok(value
            .as_str()
            .map(ToString::to_string)
            .unwrap_or_else(|| value.to_string()))
    }

    #[cfg(feature = "webauthn")]
    async fn insert_webauthn_passkey(
        &self,
        user: &UserRecord,
        credential_id: &str,
        passkey_json: String,
        label: &str,
    ) -> Result<(), Status> {
        let pool = self.require_pg_pool()?;
        let model = webauthn_credential_model();
        let rel = &model.relation;
        sqlx::query(&format!(
            "INSERT INTO {rel} ({}, {}, {}, {}, {}, {}) VALUES ($1, $2::UUID, $3::JSONB, $4, $5, $6)",
            model.q("credential_id"), model.q("user_id"), model.q("passkey_json"),
            model.q("label"), model.q("tenant_id"), model.q("project_id"),
        ))
        .bind(credential_id)
        .bind(&user.user_id)
        .bind(passkey_json)
        .bind(label)
        .bind(&user.tenant_id)
        .bind(&user.project_id)
        .execute(pool)
        .await
        .map_err(|err| Status::already_exists(format!("store WebAuthn passkey failed: {err}")))?;
        Ok(())
    }

    #[cfg(feature = "webauthn")]
    async fn update_webauthn_passkey_after_auth(
        &self,
        credential_id: &str,
        passkey_json: String,
    ) -> Result<(), Status> {
        let pool = self.require_pg_pool()?;
        let model = webauthn_credential_model();
        let rel = &model.relation;
        sqlx::query(&format!(
            "UPDATE {rel} SET {} = $2::JSONB, {} = NOW(), {} = NOW() WHERE {} = $1",
            model.q("passkey_json"),
            model.q("updated_at"),
            model.q("last_used_at"),
            model.q("credential_id"),
        ))
        .bind(credential_id)
        .bind(passkey_json)
        .execute(pool)
        .await
        .map_err(|err| Status::internal(format!("update WebAuthn passkey failed: {err}")))?;
        Ok(())
    }

    #[cfg(feature = "webauthn")]
    async fn start_webauthn_registration_impl(
        &self,
        req: authn_pb::StartWebAuthnRegistrationRequest,
    ) -> Result<authn_pb::StartWebAuthnRegistrationResponse, Status> {
        use webauthn_rs::prelude::{CredentialID, Passkey};

        if req.user_id.trim().is_empty() {
            return Err(Status::invalid_argument("user_id is required"));
        }
        let user = self
            .users
            .get_user_by_id(&req.user_id)
            .await
            .map_err(Status::internal)?
            .ok_or_else(|| Status::not_found("user not found"))?;
        if !req.tenant_id.trim().is_empty() && req.tenant_id != user.tenant_id {
            return Err(Status::permission_denied("tenant_id does not match user"));
        }
        if !req.project_id.trim().is_empty() && req.project_id != user.project_id {
            return Err(Status::permission_denied("project_id does not match user"));
        }

        let exclude_credentials = self
            .load_webauthn_passkeys(&user.user_id)
            .await?
            .iter()
            .map(|rec| {
                serde_json::from_str::<Passkey>(&rec.passkey_json)
                    .map(|passkey| passkey.cred_id().clone())
                    .map_err(|err| {
                        Status::internal(format!("decode WebAuthn passkey failed: {err}"))
                    })
            })
            .collect::<Result<Vec<CredentialID>, Status>>()?;
        let webauthn = self.webauthn()?;
        let user_uuid = Uuid::parse_str(&user.user_id)
            .map_err(|_| Status::failed_precondition("WebAuthn users must have UUID user_id"))?;
        let display_name = if user.full_name.trim().is_empty() {
            user.username.as_str()
        } else {
            user.full_name.as_str()
        };
        let (creation, state) = webauthn
            .start_passkey_registration(
                user_uuid,
                &user.username,
                display_name,
                Some(exclude_credentials),
            )
            .map_err(|err| {
                Status::internal(format!("start WebAuthn registration failed: {err:?}"))
            })?;
        let creation_json = serde_json::to_string(&creation).map_err(|err| {
            Status::internal(format!(
                "serialize WebAuthn registration challenge failed: {err}"
            ))
        })?;
        let state_json = serde_json::to_string(&WebAuthnStateEnvelope {
            state,
            label: req.label,
        })
        .map_err(|err| {
            Status::internal(format!(
                "serialize WebAuthn registration state failed: {err}"
            ))
        })?;
        let cfg = self.webauthn_config()?;
        let expires_at_unix = now_unix().saturating_add(cfg.challenge_ttl_secs);
        let challenge_id = self
            .store_webauthn_challenge(&user, "registration", state_json, expires_at_unix)
            .await?;
        Ok(authn_pb::StartWebAuthnRegistrationResponse {
            challenge_id,
            public_key_credential_creation_options_json: creation_json,
            expires_at_unix: expires_at_unix as i64,
        })
    }

    #[cfg(feature = "webauthn")]
    async fn finish_webauthn_registration_impl(
        &self,
        req: authn_pb::FinishWebAuthnRegistrationRequest,
    ) -> Result<authn_pb::FinishWebAuthnRegistrationResponse, Status> {
        use webauthn_rs::prelude::{PasskeyRegistration, RegisterPublicKeyCredential};

        if req.challenge_id.trim().is_empty() || req.public_key_credential_json.trim().is_empty() {
            return Err(Status::invalid_argument(
                "challenge_id and public_key_credential_json are required",
            ));
        }
        let challenge = self
            .load_webauthn_challenge(&req.challenge_id, "registration")
            .await?;
        let mut user = self
            .users
            .get_user_by_id(&challenge.user_id)
            .await
            .map_err(Status::internal)?
            .ok_or_else(|| Status::not_found("user not found"))?;
        if challenge.tenant_id != user.tenant_id || challenge.project_id != user.project_id {
            return Err(Status::permission_denied("invalid WebAuthn ceremony"));
        }
        let envelope: WebAuthnStateEnvelope<PasskeyRegistration> =
            serde_json::from_str(&challenge.state_json).map_err(|err| {
                Status::internal(format!("decode WebAuthn registration state failed: {err}"))
            })?;
        let credential: RegisterPublicKeyCredential =
            serde_json::from_str(&req.public_key_credential_json).map_err(|err| {
                Status::invalid_argument(format!(
                    "invalid WebAuthn registration credential JSON: {err}"
                ))
            })?;
        let passkey = self
            .webauthn()?
            .finish_passkey_registration(&credential, &envelope.state)
            .map_err(|_| Status::unauthenticated("invalid credential"))?;
        let credential_id = Self::webauthn_credential_id_text(passkey.cred_id())?;
        let passkey_json = serde_json::to_string(&passkey)
            .map_err(|err| Status::internal(format!("serialize WebAuthn passkey failed: {err}")))?;
        let label = if req.label.trim().is_empty() {
            envelope.label.as_str()
        } else {
            req.label.as_str()
        };
        self.insert_webauthn_passkey(&user, &credential_id, passkey_json, label)
            .await?;
        self.consume_webauthn_challenge(&req.challenge_id).await?;
        user.mfa_enabled = true;
        user.updated_at_unix = now_unix();
        self.users
            .put_user(user.clone())
            .await
            .map_err(Status::internal)?;
        // Audit the passkey registration (security-sensitive). The credential id
        // is a public handle, never key material.
        self.emit_event(
            AuthEvent::new(
                topics::WEBAUTHN_REGISTERED,
                user.user_id.clone(),
                user.tenant_id.clone(),
                serde_json::json!({
                    "user_id": user.user_id.clone(),
                    "credential_id": credential_id.clone(),
                }),
            )
            .with_correlation(format!("webauthn_register:{}", user.user_id))
            .with_compliance(ComplianceEnvelope {
                actor: user.user_id.clone(),
                actor_project: user.project_id.clone(),
                target_resource: credential_id.clone(),
                operation: "webauthn_register".to_string(),
                outcome: "success".to_string(),
                reason_code: "passkey_registered".to_string(),
                auth_method: "passkey".to_string(),
                ..ComplianceEnvelope::default()
            }),
        )
        .await;
        Ok(authn_pb::FinishWebAuthnRegistrationResponse {
            registered: true,
            credential_id,
            user_id: user.user_id,
        })
    }

    #[cfg(feature = "webauthn")]
    async fn start_webauthn_authentication_impl(
        &self,
        req: authn_pb::StartWebAuthnAuthenticationRequest,
    ) -> Result<authn_pb::StartWebAuthnAuthenticationResponse, Status> {
        use webauthn_rs::prelude::Passkey;

        if req.user_id.trim().is_empty() {
            return Err(Status::invalid_argument("user_id is required"));
        }
        let user = self
            .users
            .get_user_by_id(&req.user_id)
            .await
            .map_err(Status::internal)?
            .ok_or_else(|| Status::not_found("user not found"))?;
        if !req.tenant_id.trim().is_empty() && req.tenant_id != user.tenant_id {
            return Err(Status::permission_denied("tenant_id does not match user"));
        }
        if !req.project_id.trim().is_empty() && req.project_id != user.project_id {
            return Err(Status::permission_denied("project_id does not match user"));
        }
        let passkeys = self
            .load_webauthn_passkeys(&user.user_id)
            .await?
            .into_iter()
            .map(|rec| {
                serde_json::from_str::<Passkey>(&rec.passkey_json).map_err(|err| {
                    Status::internal(format!("decode WebAuthn passkey failed: {err}"))
                })
            })
            .collect::<Result<Vec<_>, Status>>()?;
        if passkeys.is_empty() {
            return Err(Status::failed_precondition(
                "user has no registered WebAuthn passkeys",
            ));
        }
        let (request_options, state) = self
            .webauthn()?
            .start_passkey_authentication(&passkeys)
            .map_err(|err| {
                Status::internal(format!("start WebAuthn authentication failed: {err:?}"))
            })?;
        let request_json = serde_json::to_string(&request_options).map_err(|err| {
            Status::internal(format!(
                "serialize WebAuthn authentication challenge failed: {err}"
            ))
        })?;
        let state_json = serde_json::to_string(&WebAuthnStateEnvelope {
            state,
            label: String::new(),
        })
        .map_err(|err| {
            Status::internal(format!(
                "serialize WebAuthn authentication state failed: {err}"
            ))
        })?;
        let cfg = self.webauthn_config()?;
        let expires_at_unix = now_unix().saturating_add(cfg.challenge_ttl_secs);
        let challenge_id = self
            .store_webauthn_challenge(&user, "authentication", state_json, expires_at_unix)
            .await?;
        Ok(authn_pb::StartWebAuthnAuthenticationResponse {
            challenge_id,
            public_key_credential_request_options_json: request_json,
            expires_at_unix: expires_at_unix as i64,
        })
    }

    #[cfg(feature = "webauthn")]
    async fn finish_webauthn_authentication_impl(
        &self,
        req: authn_pb::FinishWebAuthnAuthenticationRequest,
    ) -> Result<authn_pb::FinishWebAuthnAuthenticationResponse, Status> {
        use webauthn_rs::prelude::{Passkey, PasskeyAuthentication, PublicKeyCredential};

        if req.challenge_id.trim().is_empty() || req.public_key_credential_json.trim().is_empty() {
            return Err(Status::invalid_argument(
                "challenge_id and public_key_credential_json are required",
            ));
        }
        let challenge = self
            .load_webauthn_challenge(&req.challenge_id, "authentication")
            .await?;
        let user = self
            .users
            .get_user_by_id(&challenge.user_id)
            .await
            .map_err(Status::internal)?
            .ok_or_else(|| Status::not_found("user not found"))?;
        if challenge.tenant_id != user.tenant_id || challenge.project_id != user.project_id {
            return Err(Status::permission_denied("invalid WebAuthn ceremony"));
        }
        let envelope: WebAuthnStateEnvelope<PasskeyAuthentication> =
            serde_json::from_str(&challenge.state_json).map_err(|err| {
                Status::internal(format!(
                    "decode WebAuthn authentication state failed: {err}"
                ))
            })?;
        let credential: PublicKeyCredential = serde_json::from_str(&req.public_key_credential_json)
            .map_err(|err| {
                Status::invalid_argument(format!(
                    "invalid WebAuthn authentication credential JSON: {err}"
                ))
            })?;
        let result = self
            .webauthn()?
            .finish_passkey_authentication(&credential, &envelope.state)
            .map_err(|_| Status::unauthenticated("invalid credential"))?;
        if !result.user_verified() {
            return Err(Status::unauthenticated("invalid credential"));
        }
        let credential_id = Self::webauthn_credential_id_text(result.cred_id())?;
        let mut matched = None;
        for rec in self.load_webauthn_passkeys(&user.user_id).await? {
            let mut passkey: Passkey = serde_json::from_str(&rec.passkey_json).map_err(|err| {
                Status::internal(format!("decode WebAuthn passkey failed: {err}"))
            })?;
            if passkey.cred_id() == result.cred_id() {
                passkey.update_credential(&result);
                matched = Some(passkey);
                break;
            }
        }
        let passkey = matched.ok_or_else(|| Status::unauthenticated("invalid credential"))?;
        let passkey_json = serde_json::to_string(&passkey)
            .map_err(|err| Status::internal(format!("serialize WebAuthn passkey failed: {err}")))?;
        self.update_webauthn_passkey_after_auth(&credential_id, passkey_json)
            .await?;
        self.consume_webauthn_challenge(&req.challenge_id).await?;
        let now = now_unix();
        let (session_id, session_expires) = self
            .create_login_session(&user, "webauthn".to_string(), now)
            .await?;
        let (access_token, access_exp) = self.issue_access_token(
            &user.user_id,
            &user.tenant_id,
            &user.project_id,
            &[],
            &[],
            "",
            &session_id,
            "webauthn",
            now,
        );
        let expires_at = if access_exp > 0 {
            access_exp
        } else {
            session_expires as i64
        };
        let principal = principal_from_user_record(&user, authn::AuthnMethod::WebAuthn);
        // Audit the passkey authentication (security-sensitive).
        self.emit_event(
            AuthEvent::new(
                topics::WEBAUTHN_AUTHENTICATED,
                user.user_id.clone(),
                user.tenant_id.clone(),
                serde_json::json!({
                    "user_id": user.user_id.clone(),
                    "credential_id": credential_id.clone(),
                }),
            )
            .with_correlation(format!("webauthn_auth:{}", user.user_id))
            .with_compliance(ComplianceEnvelope {
                actor: user.user_id.clone(),
                actor_project: user.project_id.clone(),
                target_resource: credential_id.clone(),
                operation: "webauthn_authenticate".to_string(),
                outcome: "success".to_string(),
                reason_code: "passkey_verified".to_string(),
                auth_method: "passkey".to_string(),
                assurance_level: "aal2".to_string(),
                ..ComplianceEnvelope::default()
            }),
        )
        .await;
        Ok(authn_pb::FinishWebAuthnAuthenticationResponse {
            principal: Some(authn_principal_to_pb(&principal, expires_at)),
            session_id,
            access_token,
            expires_at_unix: expires_at,
            credential_id,
        })
    }
}

#[tonic::async_trait]
impl AuthnService for AuthnServiceImpl {
    async fn authenticate(
        &self,
        request: Request<authn_pb::AuthnRequest>,
    ) -> Result<Response<authn_pb::AuthnResponse>, Status> {
        self.authenticate_impl(request).await
    }

    async fn create_session(
        &self,
        request: Request<authn_pb::CreateSessionRequest>,
    ) -> Result<Response<authn_pb::CreateSessionResponse>, Status> {
        self.create_session_impl(request).await
    }

    async fn refresh_session(
        &self,
        request: Request<authn_pb::RefreshSessionRequest>,
    ) -> Result<Response<authn_pb::RefreshSessionResponse>, Status> {
        self.refresh_session_impl(request).await
    }

    async fn revoke_session(
        &self,
        request: Request<authn_pb::RevokeSessionRequest>,
    ) -> Result<Response<authn_pb::RevokeSessionResponse>, Status> {
        self.revoke_session_impl(request).await
    }

    async fn create_user(
        &self,
        request: Request<authn_pb::CreateUserRequest>,
    ) -> Result<Response<authn_pb::CreateUserResponse>, Status> {
        self.create_user_impl(request).await
    }
    async fn get_user(
        &self,
        request: Request<authn_pb::GetUserRequest>,
    ) -> Result<Response<authn_pb::GetUserResponse>, Status> {
        self.get_user_impl(request).await
    }
    async fn list_users(
        &self,
        request: Request<authn_pb::ListUsersRequest>,
    ) -> Result<Response<authn_pb::ListUsersResponse>, Status> {
        self.list_users_impl(request).await
    }
    async fn update_user(
        &self,
        request: Request<authn_pb::UpdateUserRequest>,
    ) -> Result<Response<authn_pb::UpdateUserResponse>, Status> {
        self.update_user_impl(request).await
    }
    async fn change_user_status(
        &self,
        request: Request<authn_pb::ChangeUserStatusRequest>,
    ) -> Result<Response<authn_pb::ChangeUserStatusResponse>, Status> {
        self.change_user_status_impl(request).await
    }
    async fn admin_reset_password(
        &self,
        request: Request<authn_pb::AdminResetPasswordRequest>,
    ) -> Result<Response<authn_pb::AdminResetPasswordResponse>, Status> {
        self.admin_reset_password_impl(request).await
    }
    async fn send_otp(
        &self,
        request: Request<authn_pb::SendOtpRequest>,
    ) -> Result<Response<authn_pb::SendOtpResponse>, Status> {
        self.send_otp_impl(request).await
    }
    async fn verify_otp(
        &self,
        request: Request<authn_pb::VerifyOtpRequest>,
    ) -> Result<Response<authn_pb::VerifyOtpResponse>, Status> {
        self.verify_otp_impl(request).await
    }
    async fn resend_otp(
        &self,
        request: Request<authn_pb::ResendOtpRequest>,
    ) -> Result<Response<authn_pb::ResendOtpResponse>, Status> {
        self.resend_otp_impl(request).await
    }
    async fn login(
        &self,
        request: Request<authn_pb::LoginRequest>,
    ) -> Result<Response<authn_pb::LoginResponse>, Status> {
        self.login_impl(request).await
    }
    async fn refresh_token(
        &self,
        request: Request<authn_pb::RefreshTokenRequest>,
    ) -> Result<Response<authn_pb::RefreshTokenResponse>, Status> {
        self.refresh_token_impl(request).await
    }
    async fn logout(
        &self,
        request: Request<authn_pb::LogoutRequest>,
    ) -> Result<Response<authn_pb::LogoutResponse>, Status> {
        self.logout_impl(request).await
    }
    async fn change_password(
        &self,
        request: Request<authn_pb::ChangePasswordRequest>,
    ) -> Result<Response<authn_pb::ChangePasswordResponse>, Status> {
        self.change_password_impl(request).await
    }
    async fn validate_token(
        &self,
        request: Request<authn_pb::ValidateTokenRequest>,
    ) -> Result<Response<authn_pb::ValidateTokenResponse>, Status> {
        self.validate_token_impl(request).await
    }
    async fn get_session(
        &self,
        request: Request<authn_pb::GetSessionRequest>,
    ) -> Result<Response<authn_pb::GetSessionResponse>, Status> {
        self.get_session_impl(request).await
    }
    async fn list_sessions(
        &self,
        request: Request<authn_pb::ListSessionsRequest>,
    ) -> Result<Response<authn_pb::ListSessionsResponse>, Status> {
        self.list_sessions_impl(request).await
    }
    async fn validate_csrf(
        &self,
        request: Request<authn_pb::ValidateCsrfRequest>,
    ) -> Result<Response<authn_pb::ValidateCsrfResponse>, Status> {
        self.validate_csrf_impl(request).await
    }
    async fn enroll_mfa(
        &self,
        request: Request<authn_pb::EnrollMfaRequest>,
    ) -> Result<Response<authn_pb::EnrollMfaResponse>, Status> {
        self.enroll_mfa_impl(request).await
    }
    async fn confirm_mfa_enrollment(
        &self,
        request: Request<authn_pb::ConfirmMfaEnrollmentRequest>,
    ) -> Result<Response<authn_pb::ConfirmMfaEnrollmentResponse>, Status> {
        self.confirm_mfa_enrollment_impl(request).await
    }

    async fn generate_recovery_codes(
        &self,
        request: Request<authn_pb::GenerateRecoveryCodesRequest>,
    ) -> Result<Response<authn_pb::GenerateRecoveryCodesResponse>, Status> {
        self.generate_recovery_codes_impl(request).await
    }

    async fn put_mfa_policy(
        &self,
        request: Request<authn_pb::PutMfaPolicyRequest>,
    ) -> Result<Response<authn_pb::PutMfaPolicyResponse>, Status> {
        self.put_mfa_policy_impl(request).await
    }

    async fn get_mfa_policy(
        &self,
        request: Request<authn_pb::GetMfaPolicyRequest>,
    ) -> Result<Response<authn_pb::GetMfaPolicyResponse>, Status> {
        self.get_mfa_policy_impl(request).await
    }

    async fn forgot_password(
        &self,
        request: Request<authn_pb::ForgotPasswordRequest>,
    ) -> Result<Response<authn_pb::ForgotPasswordResponse>, Status> {
        self.forgot_password_impl(request).await
    }

    async fn reset_password(
        &self,
        request: Request<authn_pb::ResetPasswordRequest>,
    ) -> Result<Response<authn_pb::ResetPasswordResponse>, Status> {
        self.reset_password_impl(request).await
    }

    async fn introspect_token(
        &self,
        request: Request<authn_pb::IntrospectTokenRequest>,
    ) -> Result<Response<authn_pb::IntrospectTokenResponse>, Status> {
        self.introspect_token_impl(request).await
    }

    async fn get_jwks(
        &self,
        request: Request<authn_pb::GetJwksRequest>,
    ) -> Result<Response<authn_pb::GetJwksResponse>, Status> {
        self.get_jwks_impl(request).await
    }

    async fn send_phone_verification(
        &self,
        request: Request<authn_pb::SendPhoneVerificationRequest>,
    ) -> Result<Response<authn_pb::SendPhoneVerificationResponse>, Status> {
        self.send_phone_verification_impl(request).await
    }

    async fn start_web_authn_registration(
        &self,
        request: Request<authn_pb::StartWebAuthnRegistrationRequest>,
    ) -> Result<Response<authn_pb::StartWebAuthnRegistrationResponse>, Status> {
        #[cfg(feature = "webauthn")]
        {
            self.start_webauthn_registration_impl(request.into_inner())
                .await
                .map(Response::new)
        }
        #[cfg(not(feature = "webauthn"))]
        {
            let _ = request;
            Err(Status::failed_precondition(
                "WebAuthn requires building UDB with the `webauthn` feature",
            ))
        }
    }

    async fn finish_web_authn_registration(
        &self,
        request: Request<authn_pb::FinishWebAuthnRegistrationRequest>,
    ) -> Result<Response<authn_pb::FinishWebAuthnRegistrationResponse>, Status> {
        #[cfg(feature = "webauthn")]
        {
            self.finish_webauthn_registration_impl(request.into_inner())
                .await
                .map(Response::new)
        }
        #[cfg(not(feature = "webauthn"))]
        {
            let _ = request;
            Err(Status::failed_precondition(
                "WebAuthn requires building UDB with the `webauthn` feature",
            ))
        }
    }

    async fn start_web_authn_authentication(
        &self,
        request: Request<authn_pb::StartWebAuthnAuthenticationRequest>,
    ) -> Result<Response<authn_pb::StartWebAuthnAuthenticationResponse>, Status> {
        #[cfg(feature = "webauthn")]
        {
            self.start_webauthn_authentication_impl(request.into_inner())
                .await
                .map(Response::new)
        }
        #[cfg(not(feature = "webauthn"))]
        {
            let _ = request;
            Err(Status::failed_precondition(
                "WebAuthn requires building UDB with the `webauthn` feature",
            ))
        }
    }

    async fn finish_web_authn_authentication(
        &self,
        request: Request<authn_pb::FinishWebAuthnAuthenticationRequest>,
    ) -> Result<Response<authn_pb::FinishWebAuthnAuthenticationResponse>, Status> {
        #[cfg(feature = "webauthn")]
        {
            self.finish_webauthn_authentication_impl(request.into_inner())
                .await
                .map(Response::new)
        }
        #[cfg(not(feature = "webauthn"))]
        {
            let _ = request;
            Err(Status::failed_precondition(
                "WebAuthn requires building UDB with the `webauthn` feature",
            ))
        }
    }

    // ── Device + session revocation lifecycle (Phase 3 / I2.4) ───────────────
    async fn list_devices(
        &self,
        request: Request<authn_pb::ListDevicesRequest>,
    ) -> Result<Response<authn_pb::ListDevicesResponse>, Status> {
        self.list_devices_impl(request).await
    }
    async fn revoke_device(
        &self,
        request: Request<authn_pb::RevokeDeviceRequest>,
    ) -> Result<Response<authn_pb::RevokeDeviceResponse>, Status> {
        self.revoke_device_impl(request).await
    }
    async fn admin_revoke_session(
        &self,
        request: Request<authn_pb::AdminRevokeSessionRequest>,
    ) -> Result<Response<authn_pb::AdminRevokeSessionResponse>, Status> {
        self.admin_revoke_session_impl(request).await
    }
    async fn admin_revoke_all_user_sessions(
        &self,
        request: Request<authn_pb::AdminRevokeAllUserSessionsRequest>,
    ) -> Result<Response<authn_pb::AdminRevokeAllUserSessionsResponse>, Status> {
        self.admin_revoke_all_user_sessions_impl(request).await
    }
    async fn admin_revoke_all_tenant_sessions(
        &self,
        request: Request<authn_pb::AdminRevokeAllTenantSessionsRequest>,
    ) -> Result<Response<authn_pb::AdminRevokeAllTenantSessionsResponse>, Status> {
        self.admin_revoke_all_tenant_sessions_impl(request).await
    }
    async fn emergency_revoke(
        &self,
        request: Request<authn_pb::EmergencyRevokeRequest>,
    ) -> Result<Response<authn_pb::EmergencyRevokeResponse>, Status> {
        self.emergency_revoke_impl(request).await
    }

    // ── MFA challenge + factor lifecycle (Phase 3 / I2.6) ────────────────────
    async fn issue_mfa_challenge(
        &self,
        request: Request<authn_pb::IssueMfaChallengeRequest>,
    ) -> Result<Response<authn_pb::IssueMfaChallengeResponse>, Status> {
        self.issue_mfa_challenge_impl(request).await
    }
    async fn verify_mfa_challenge(
        &self,
        request: Request<authn_pb::VerifyMfaChallengeRequest>,
    ) -> Result<Response<authn_pb::VerifyMfaChallengeResponse>, Status> {
        self.verify_mfa_challenge_impl(request).await
    }
    async fn list_mfa_factors(
        &self,
        request: Request<authn_pb::ListMfaFactorsRequest>,
    ) -> Result<Response<authn_pb::ListMfaFactorsResponse>, Status> {
        self.list_mfa_factors_impl(request).await
    }
    async fn disable_mfa_factor(
        &self,
        request: Request<authn_pb::DisableMfaFactorRequest>,
    ) -> Result<Response<authn_pb::DisableMfaFactorResponse>, Status> {
        self.disable_mfa_factor_impl(request).await
    }
    async fn rename_passkey(
        &self,
        request: Request<authn_pb::RenamePasskeyRequest>,
    ) -> Result<Response<authn_pb::RenamePasskeyResponse>, Status> {
        self.rename_passkey_impl(request).await
    }
    async fn revoke_recovery_codes(
        &self,
        request: Request<authn_pb::RevokeRecoveryCodesRequest>,
    ) -> Result<Response<authn_pb::RevokeRecoveryCodesResponse>, Status> {
        self.revoke_recovery_codes_impl(request).await
    }
    async fn admin_reset_mfa(
        &self,
        request: Request<authn_pb::AdminResetMfaRequest>,
    ) -> Result<Response<authn_pb::AdminResetMfaResponse>, Status> {
        self.admin_reset_mfa_impl(request).await
    }

    // ── WebAuthn enterprise credential lifecycle (Phase 3 / I2.7) ────────────
    async fn list_web_authn_credentials(
        &self,
        request: Request<authn_pb::ListWebAuthnCredentialsRequest>,
    ) -> Result<Response<authn_pb::ListWebAuthnCredentialsResponse>, Status> {
        self.list_web_authn_credentials_impl(request).await
    }
    async fn delete_web_authn_credential(
        &self,
        request: Request<authn_pb::DeleteWebAuthnCredentialRequest>,
    ) -> Result<Response<authn_pb::DeleteWebAuthnCredentialResponse>, Status> {
        self.delete_web_authn_credential_impl(request).await
    }
}
