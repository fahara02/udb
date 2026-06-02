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

use super::events::{self, AuthEvent, AuthEventSink, topics};
use super::mappings::{
    authn_principal_to_pb, bounded_page_response, bounded_page_window, principal_from_api_key,
    principal_from_session, session_record_to_pb, timestamp_from_unix,
};
use super::now_unix;

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

fn validate_session_response(
    rec: Option<SessionRecord>,
    raw_session_id: &str,
    now_unix: u64,
) -> authn_pb::ValidateTokenResponse {
    let Some(rec) = rec else {
        return authn_pb::ValidateTokenResponse {
            valid: false,
            ..Default::default()
        };
    };
    let principal = principal_from_session(&rec);
    authn_pb::ValidateTokenResponse {
        valid: true,
        user_id: rec.user_id.clone(),
        session_id: raw_session_id.to_string(),
        account_kind: authn_entity_pb::AccountKind::Unspecified as i32,
        tenant_id: rec.tenant_id.clone(),
        roles: rec.roles.clone(),
        expires_at: timestamp_from_unix(rec.expires_at_unix),
        access_surface: "session".to_string(),
        device_id: rec.client_fingerprint.clone(),
        token_id: rec.session_id_hash.chars().take(24).collect(),
        session_type: authn_entity_pb::SessionType::ServerSide as i32,
        principal: Some(authn_principal_to_pb(
            &principal,
            rec.expires_at_unix as i64,
        )),
        project_id: rec.project_id.clone(),
        scopes: rec.scopes.clone(),
        attributes: [
            ("active".to_string(), rec.is_active(now_unix).to_string()),
            (
                "relationship_version".to_string(),
                rec.relationship_version.clone(),
            ),
        ]
        .into_iter()
        .collect(),
    }
}

fn validate_api_key_response(rec: Option<authn::ApiKeyRecord>) -> authn_pb::ValidateTokenResponse {
    let Some(rec) = rec else {
        return authn_pb::ValidateTokenResponse {
            valid: false,
            ..Default::default()
        };
    };
    let principal = principal_from_api_key(&rec);
    authn_pb::ValidateTokenResponse {
        valid: true,
        user_id: String::new(),
        session_id: String::new(),
        account_kind: authn_entity_pb::AccountKind::ServiceAccount as i32,
        tenant_id: rec.tenant_id.clone(),
        roles: Vec::new(),
        expires_at: timestamp_from_unix(rec.expires_at_unix),
        access_surface: "api_key".to_string(),
        device_id: String::new(),
        token_id: rec.key_prefix.clone(),
        session_type: authn_entity_pb::SessionType::ApiKey as i32,
        principal: Some(authn_principal_to_pb(
            &principal,
            rec.expires_at_unix as i64,
        )),
        project_id: rec.project_id.clone(),
        scopes: rec.scopes.clone(),
        attributes: Default::default(),
    }
}

fn user_record_to_pb(rec: &UserRecord) -> authn_entity_pb::User {
    authn_entity_pb::User {
        user_id: rec.user_id.clone(),
        username: rec.username.clone(),
        email: rec.email.clone(),
        password_hash: rec.password_hash.clone(),
        account_kind: rec.account_kind,
        status: rec.status,
        tenant_id: rec.tenant_id.clone(),
        full_name: rec.full_name.clone(),
        totp_secret_enc: rec.totp_secret_hash.clone(),
        mfa_enabled: rec.mfa_enabled,
        failed_login_count: rec.failed_login_count,
        locked_until: timestamp_from_unix(rec.locked_until_unix),
        email_verified_at: timestamp_from_unix(rec.email_verified_at_unix),
        last_login_at: timestamp_from_unix(rec.last_login_at_unix),
        created_by: rec.created_by.clone(),
        created_at: timestamp_from_unix(rec.created_at_unix),
        updated_at: timestamp_from_unix(rec.updated_at_unix),
        deleted_at: timestamp_from_unix(rec.deleted_at_unix),
        deleted_by: rec.deleted_by.clone(),
        project_id: rec.project_id.clone(),
        external_provider_id: rec.external_provider_id.clone(),
        external_subject: rec.external_subject.clone(),
        locale: String::new(),
        timezone: String::new(),
        profile_attributes_json: rec.profile_attributes_json.clone(),
        external_references_json: "[]".to_string(),
    }
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

fn generated_code(_seed: &str, _now: u64) -> String {
    // Draw the 6-digit code from the CSPRNG-backed UUID generator rather than a
    // deterministic time+seed HMAC fold. The `u128 % 1_000_000` modulo bias is
    // negligible (2^128 ≫ 10^6). The code is hashed before storage, so it never
    // needs to be recomputable.
    let n = (Uuid::new_v4().as_u128() % 1_000_000) as u32;
    format!("{n:06}")
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
        }
    }

    pub fn with_postgres(mut self, pool: Option<PgPool>) -> Self {
        self.pg_pool = pool;
        self
    }

    /// Attach the domain-event sink (outbox → Kafka). Defaults to a no-op.
    pub(crate) fn with_event_sink(mut self, sink: Arc<dyn AuthEventSink>) -> Self {
        self.event_sink = sink;
        self
    }

    /// Emit an authn domain event, logging (never failing the RPC) on error.
    async fn emit_event(&self, event: AuthEvent) {
        let topic = event.topic;
        if let Err(err) = self.event_sink.emit(event).await {
            tracing::warn!(topic, error = %err, "failed to publish authn event");
        }
    }

    fn hash_key(&self) -> Vec<u8> {
        self.config.session_hash_secret.as_bytes().to_vec()
    }

    fn api_key_hash_key(&self) -> Vec<u8> {
        self.config.api_key_hash_secret().as_bytes().to_vec()
    }

    fn password_hash_key(&self) -> Vec<u8> {
        self.config.password_hash_secret().as_bytes().to_vec()
    }

    fn otp_hash_key(&self) -> Vec<u8> {
        self.config.otp_hash_secret().as_bytes().to_vec()
    }

    /// Derive the CSRF token bound to a session id (signed double-submit cookie).
    /// Issued at login and re-derived for validation; never stored.
    fn csrf_token_for(&self, session_id: &str) -> String {
        authn::hash_secret(&format!("csrf:{session_id}"), &self.hash_key())
    }

    /// Issue a UDB-signed RS256 access token for a session when a signing key is
    /// configured (`UDB_JWT_PRIVATE_KEY`). Returns `(access_token,
    /// expires_at_unix)`; an empty token + `0` expiry when JWT signing is not
    /// configured, so callers transparently fall back to session-only auth.
    #[allow(clippy::too_many_arguments)]
    fn issue_access_token(
        &self,
        subject: &str,
        tenant_id: &str,
        project_id: &str,
        scopes: &[String],
        roles: &[String],
        service_identity: &str,
        jti: &str,
        now: u64,
    ) -> (String, i64) {
        match crate::runtime::security::sign_access_token(
            &self.security,
            subject,
            tenant_id,
            project_id,
            scopes,
            roles,
            service_identity,
            jti,
            now,
        ) {
            Ok(Some((token, exp))) => (token, exp),
            Ok(None) => (String::new(), 0),
            Err(err) => {
                tracing::warn!(error = %err, "access-token signing failed; falling back to session-only");
                (String::new(), 0)
            }
        }
    }

    async fn issue_otp(
        &self,
        user: &UserRecord,
        otp_type: i32,
        correlation_id: String,
        now: u64,
    ) -> Result<(String, String), Status> {
        let otp_id = Uuid::new_v4().to_string();
        let code = generated_code(&format!("{otp_id}:{}", user.user_id), now);
        let rec = OtpRecord {
            otp_id: otp_id.clone(),
            user_id: user.user_id.clone(),
            otp_type,
            code_hash: authn::hash_otp_code(&code, &self.otp_hash_key()),
            delivery_channel: "email".to_string(),
            delivery_address: user.email.clone(),
            status: authn_entity_pb::OtpStatus::Pending as i32,
            attempt_count: 0,
            superseded_by_id: String::new(),
            expires_at_unix: now.saturating_add(self.config.otp_ttl_secs),
            used_at_unix: 0,
            created_at_unix: now,
            correlation_id,
        };
        self.users.put_otp(rec).await.map_err(Status::internal)?;
        #[cfg(test)]
        if let Ok(mut codes) = test_otp_codes().lock() {
            codes.insert(otp_id.clone(), code.clone());
        }
        Ok((otp_id, code))
    }

    async fn verify_otp_record(
        &self,
        otp_id: &str,
        code: &str,
        expected_type: Option<i32>,
        now: u64,
    ) -> Result<Option<OtpRecord>, Status> {
        let Some(mut rec) = self.users.get_otp(otp_id).await.map_err(Status::internal)? else {
            return Ok(None);
        };
        if expected_type.is_some_and(|kind| rec.otp_type != kind) {
            return Ok(None);
        }
        if rec.status != authn_entity_pb::OtpStatus::Pending as i32 || now >= rec.expires_at_unix {
            rec.status = authn_entity_pb::OtpStatus::Expired as i32;
            self.users.update_otp(rec).await.map_err(Status::internal)?;
            return Ok(None);
        }
        if !authn::verify_otp_code(code, &self.otp_hash_key(), &rec.code_hash) {
            rec.attempt_count += 1;
            if rec.attempt_count >= 5 {
                rec.status = authn_entity_pb::OtpStatus::Expired as i32;
            }
            self.users.update_otp(rec).await.map_err(Status::internal)?;
            return Ok(None);
        }
        rec.status = authn_entity_pb::OtpStatus::Used as i32;
        rec.used_at_unix = now;
        self.users
            .update_otp(rec.clone())
            .await
            .map_err(Status::internal)?;
        Ok(Some(rec))
    }

    async fn create_login_session(
        &self,
        user: &UserRecord,
        client_fingerprint: String,
        now: u64,
    ) -> Result<(String, u64), Status> {
        if !self.config.sessions_usable() {
            return Err(Status::failed_precondition(
                "sessions disabled (set UDB_SESSION_ENABLED and UDB_SESSION_HASH_SECRET)",
            ));
        }
        let raw_session_id = format!("sess_{}", Uuid::new_v4().simple());
        let expires = now.saturating_add(self.config.session_ttl_secs);
        let rec = SessionRecord {
            session_id_hash: authn::hash_secret(&raw_session_id, &self.hash_key()),
            principal_id: user.user_id.clone(),
            user_id: user.user_id.clone(),
            service_identity: String::new(),
            tenant_id: user.tenant_id.clone(),
            project_id: user.project_id.clone(),
            scopes: Vec::new(),
            roles: Vec::new(),
            relationship_version: String::new(),
            created_at_unix: now,
            updated_at_unix: now,
            expires_at_unix: expires,
            revoked_at_unix: 0,
            client_fingerprint,
        };
        self.sessions.put(&rec).await.map_err(Status::internal)?;
        Ok((raw_session_id, expires))
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
    async fn authenticate_oidc_token(
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
            return Err(Status::unauthenticated(
                "OIDC ID token must be a signed JWT",
            ));
        }
        let issuer = if !req.issuer.trim().is_empty() {
            req.issuer.trim().to_string()
        } else {
            std::env::var("UDB_OIDC_ISSUER").unwrap_or_default()
        };
        if issuer.is_empty() {
            return Err(Status::invalid_argument(
                "OIDC issuer is required (request issuer or UDB_OIDC_ISSUER)",
            ));
        }
        let client_id = if !req.client_id.trim().is_empty() {
            req.client_id.trim().to_string()
        } else if !req.audience.trim().is_empty() {
            req.audience.trim().to_string()
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
                .map_err(|err| Status::unauthenticated(format!("OIDC discovery failed: {err}")))?;
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
                .map_err(|err| Status::unauthenticated(format!("invalid OIDC ID token: {err}")))?;
            let verifier = client.id_token_verifier();
            let claims = id_token
                .claims(&verifier, &Nonce::new(nonce))
                .map_err(|err| {
                    Status::unauthenticated(format!("OIDC ID token verification failed: {err}"))
                })?;
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
        .ok_or_else(|| Status::not_found("WebAuthn challenge not found, expired, or consumed"))?;
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
            return Err(Status::permission_denied(
                "WebAuthn challenge scope does not match user",
            ));
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
            .map_err(|err| {
                Status::unauthenticated(format!(
                    "WebAuthn registration verification failed: {err:?}"
                ))
            })?;
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
            return Err(Status::permission_denied(
                "WebAuthn challenge scope does not match user",
            ));
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
            .map_err(|err| {
                Status::unauthenticated(format!(
                    "WebAuthn authentication verification failed: {err:?}"
                ))
            })?;
        if !result.user_verified() {
            return Err(Status::unauthenticated(
                "WebAuthn assertion did not provide user verification",
            ));
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
        let passkey = matched.ok_or_else(|| {
            Status::unauthenticated("WebAuthn credential is not registered for user")
        })?;
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
            now,
        );
        let expires_at = if access_exp > 0 {
            access_exp
        } else {
            session_expires as i64
        };
        let principal = principal_from_user_record(&user, authn::AuthnMethod::WebAuthn);
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
        let req = request.into_inner();
        let now = now_unix();

        // API/service key.
        if !req.api_key.trim().is_empty() {
            let rec = authn::validate_api_key(
                self.api_keys.as_ref(),
                &req.api_key,
                &self.api_key_hash_key(),
                now,
            )
            .await
            .map_err(Status::internal)?
            .ok_or_else(|| Status::unauthenticated("invalid or expired api key"))?;
            let principal = principal_from_api_key(&rec);
            return Ok(Response::new(authn_pb::AuthnResponse {
                principal: Some(authn_principal_to_pb(
                    &principal,
                    rec.expires_at_unix as i64,
                )),
                session_id: String::new(),
                access_token: String::new(),
                expires_at_unix: rec.expires_at_unix as i64,
                relationship_version: String::new(),
                warnings: Vec::new(),
            }));
        }

        // Server-side session.
        if !req.session_id.trim().is_empty() {
            let rec = authn::validate_session(
                self.sessions.as_ref(),
                &req.session_id,
                &self.hash_key(),
                now,
                self.config.session_idle_ttl_secs,
            )
            .await
            .map_err(Status::internal)?
            .ok_or_else(|| Status::unauthenticated("invalid or expired session"))?;
            let principal = principal_from_session(&rec);
            return Ok(Response::new(authn_pb::AuthnResponse {
                principal: Some(authn_principal_to_pb(
                    &principal,
                    rec.expires_at_unix as i64,
                )),
                session_id: req.session_id,
                access_token: String::new(),
                expires_at_unix: rec.expires_at_unix as i64,
                relationship_version: rec.relationship_version,
                warnings: Vec::new(),
            }));
        }

        if req.credential_type == authn_entity_pb::AuthCredentialType::OidcToken as i32 {
            #[cfg(feature = "oidc")]
            {
                let principal = self.authenticate_oidc_token(&req).await?;
                return Ok(Response::new(authn_pb::AuthnResponse {
                    principal: Some(authn_principal_to_pb(&principal, 0)),
                    session_id: String::new(),
                    access_token: String::new(),
                    expires_at_unix: 0,
                    relationship_version: String::new(),
                    warnings: vec![
                        "OIDC ID token verified via provider discovery and JWKS".to_string(),
                    ],
                }));
            }
            #[cfg(not(feature = "oidc"))]
            {
                return Err(Status::failed_precondition(
                    "OIDC authentication requires building UDB with the `oidc` feature",
                ));
            }
        }

        // Hybrid external provider: verify a signed JWT first, then map the
        // verified claims to a UDB principal. Raw JSON "claims" are not accepted
        // here; trusted PEPs must pass a bearer token validated by UDB.
        if !req.external_provider_id.trim().is_empty() {
            if req.external_token.matches('.').count() != 2 {
                return Err(Status::unauthenticated(
                    "external_token must be a signed JWT, not raw claims JSON",
                ));
            }
            let claims = validate_bearer_token(&self.security, &req.external_token)
                .map_err(Status::unauthenticated)?;
            if !req.issuer.trim().is_empty()
                && claims.iss.as_deref().unwrap_or_default() != req.issuer
            {
                return Err(Status::unauthenticated("external token issuer mismatch"));
            }
            if !req.audience.trim().is_empty()
                && self.security.jwt_audience.as_deref() != Some(req.audience.as_str())
            {
                return Err(Status::unauthenticated(
                    "external token audience is not configured for validation",
                ));
            }
            let subject = claims.sub.clone().unwrap_or_default();
            let verified_claims = serde_json::json!({
                "sub": subject,
                "tenant_id": claims.tenant_id.clone().unwrap_or_default(),
                "project_id": claims.project_id.clone().unwrap_or_default(),
                "scopes": claims.resolved_scopes(),
                "roles": claims.roles.clone().unwrap_or_default(),
            });
            let provider = ExternalJwtProvider::new(ExternalProviderConfig {
                provider_id: req.external_provider_id.clone(),
                ..ExternalProviderConfig::default()
            });
            let principal = provider
                .map_identity(&subject, &verified_claims.to_string())
                .map_err(Status::unauthenticated)?;
            return Ok(Response::new(authn_pb::AuthnResponse {
                principal: Some(authn_principal_to_pb(&principal, 0)),
                session_id: String::new(),
                access_token: String::new(),
                expires_at_unix: 0,
                relationship_version: String::new(),
                warnings: vec![
                    "external identity mapped; UDB authz still governs access".to_string(),
                ],
            }));
        }

        // Native UDB JWT.
        if !req.bearer_token.trim().is_empty() {
            let claims = validate_bearer_token(&self.security, &req.bearer_token)
                .map_err(Status::unauthenticated)?;
            let subject = claims.sub.clone().unwrap_or_default();
            let principal = Principal {
                principal_id: subject.clone(),
                subject: subject.clone(),
                user_id: subject,
                service_identity: claims.service_identity.clone().unwrap_or_default(),
                tenant_id: claims
                    .tenant_id
                    .clone()
                    .filter(|t| !t.trim().is_empty())
                    .unwrap_or_else(|| req.tenant_hint.clone()),
                project_id: claims
                    .project_id
                    .clone()
                    .filter(|p| !p.trim().is_empty())
                    .unwrap_or_else(|| req.project_hint.clone()),
                scopes: claims.resolved_scopes(),
                roles: claims.roles.clone().unwrap_or_default(),
                provider_id: String::new(),
                auth_method: authn::AuthnMethod::Jwt.as_str().to_string(),
            };
            return Ok(Response::new(authn_pb::AuthnResponse {
                principal: Some(authn_principal_to_pb(&principal, 0)),
                session_id: String::new(),
                access_token: String::new(),
                expires_at_unix: 0,
                relationship_version: claims.relationships_version.unwrap_or_default(),
                warnings: Vec::new(),
            }));
        }

        Err(Status::invalid_argument(
            "no credential supplied (set api_key, session_id, bearer_token, or external_provider_id+external_token)",
        ))
    }

    async fn create_session(
        &self,
        request: Request<authn_pb::CreateSessionRequest>,
    ) -> Result<Response<authn_pb::CreateSessionResponse>, Status> {
        if !self.config.sessions_usable() {
            return Err(Status::failed_precondition(
                "sessions disabled (set UDB_SESSION_ENABLED and UDB_SESSION_HASH_SECRET)",
            ));
        }
        let req = request.into_inner();
        let p = req
            .principal
            .ok_or_else(|| Status::invalid_argument("principal is required"))?;
        let now = now_unix();
        let ttl = if req.ttl_seconds > 0 {
            req.ttl_seconds as u64
        } else {
            self.config.session_ttl_secs
        };
        let expires = now.saturating_add(ttl);
        let raw_session_id = format!("sess_{}", Uuid::new_v4().simple());
        let rec = SessionRecord {
            session_id_hash: authn::hash_secret(&raw_session_id, &self.hash_key()),
            principal_id: p.principal_id,
            user_id: p.user_id,
            service_identity: p.service_identity,
            tenant_id: p.tenant_id,
            project_id: p.project_id,
            scopes: p.scopes,
            roles: p.roles,
            relationship_version: String::new(),
            created_at_unix: now,
            updated_at_unix: now,
            expires_at_unix: expires,
            revoked_at_unix: 0,
            client_fingerprint: req.client_fingerprint,
        };
        self.sessions.put(&rec).await.map_err(Status::internal)?;
        Ok(Response::new(authn_pb::CreateSessionResponse {
            session_id: raw_session_id,
            expires_at_unix: expires as i64,
        }))
    }

    async fn refresh_session(
        &self,
        request: Request<authn_pb::RefreshSessionRequest>,
    ) -> Result<Response<authn_pb::RefreshSessionResponse>, Status> {
        let req = request.into_inner();
        let now = now_unix();
        let ttl = if req.ttl_seconds > 0 {
            req.ttl_seconds as u64
        } else {
            self.config.session_ttl_secs
        };
        match authn::refresh_session(
            self.sessions.as_ref(),
            &req.session_id,
            &self.hash_key(),
            now,
            ttl,
        )
        .await
        .map_err(Status::internal)?
        {
            Some(rec) => Ok(Response::new(authn_pb::RefreshSessionResponse {
                expires_at_unix: rec.expires_at_unix as i64,
                active: true,
            })),
            None => Ok(Response::new(authn_pb::RefreshSessionResponse {
                expires_at_unix: 0,
                active: false,
            })),
        }
    }

    async fn revoke_session(
        &self,
        request: Request<authn_pb::RevokeSessionRequest>,
    ) -> Result<Response<authn_pb::RevokeSessionResponse>, Status> {
        let req = request.into_inner();
        let now = now_unix();
        if req.all_for_principal && !req.principal_id.trim().is_empty() {
            let n = self
                .sessions
                .revoke_all_for_principal(&req.principal_id, now)
                .await
                .map_err(Status::internal)?;
            return Ok(Response::new(authn_pb::RevokeSessionResponse {
                session_id: String::new(),
                revoked_at: None,
                operation_id: Uuid::new_v4().to_string(),
                revoked_count: n as i32,
            }));
        }
        let hash = authn::hash_secret(&req.session_id, &self.hash_key());
        let ok = self
            .sessions
            .revoke(&hash, now)
            .await
            .map_err(Status::internal)?;
        if ok {
            self.emit_event(AuthEvent::new(
                topics::SESSION_REVOKED,
                req.session_id.clone(),
                String::new(),
                serde_json::json!({
                    "session_id": req.session_id.clone(),
                    "revoke_reason": req.revoke_reason.clone(),
                    "revoked_by": req.principal_id.clone(),
                }),
            ))
            .await;
        }
        Ok(Response::new(authn_pb::RevokeSessionResponse {
            session_id: req.session_id,
            revoked_at: None,
            operation_id: Uuid::new_v4().to_string(),
            revoked_count: i32::from(ok),
        }))
    }

    async fn create_user(
        &self,
        request: Request<authn_pb::CreateUserRequest>,
    ) -> Result<Response<authn_pb::CreateUserResponse>, Status> {
        if self.password_hash_key().is_empty() {
            return Err(Status::failed_precondition(
                "native user passwords require UDB_PASSWORD_HASH_SECRET or UDB_SESSION_HASH_SECRET",
            ));
        }
        let req = request.into_inner();
        if req.username.trim().is_empty() || req.email.trim().is_empty() {
            return Err(Status::invalid_argument("username and email are required"));
        }
        if req.password.len() < 10 && req.external_provider_id.trim().is_empty() {
            return Err(Status::invalid_argument(
                "password must be at least 10 characters",
            ));
        }
        let now = now_unix();
        let user_id = Uuid::new_v4().to_string();
        let account_kind = if req.account_kind == authn_entity_pb::AccountKind::Unspecified as i32 {
            authn_entity_pb::AccountKind::Person as i32
        } else {
            req.account_kind
        };
        let created_by = req
            .context
            .as_ref()
            .map(|ctx| ctx.principal_id.clone())
            .unwrap_or_default();
        let rec = UserRecord {
            user_id: user_id.clone(),
            username: req.username.trim().to_ascii_lowercase(),
            email: req.email.trim().to_ascii_lowercase(),
            password_hash: authn::hash_password(&req.password, &self.password_hash_key()),
            account_kind,
            status: authn_entity_pb::UserStatus::PendingVerification as i32,
            tenant_id: req.tenant_id,
            full_name: req.full_name,
            totp_secret_hash: String::new(),
            mfa_enabled: false,
            failed_login_count: 0,
            locked_until_unix: 0,
            email_verified_at_unix: 0,
            last_login_at_unix: 0,
            created_by,
            created_at_unix: now,
            updated_at_unix: now,
            deleted_at_unix: 0,
            deleted_by: String::new(),
            project_id: req.project_id,
            external_provider_id: req.external_provider_id,
            external_subject: req.external_subject,
            profile_attributes_json: serde_json::to_string(&req.profile_attributes)
                .unwrap_or_else(|_| "{}".to_string()),
        };
        self.users
            .put_user(rec.clone())
            .await
            .map_err(Status::internal)?;
        let (otp_id, _code) = self
            .issue_otp(
                &rec,
                authn_entity_pb::OtpType::EmailVerification as i32,
                format!("create_user:{user_id}"),
                now,
            )
            .await?;
        self.emit_event(
            AuthEvent::new(
                topics::USER_REGISTERED,
                rec.user_id.clone(),
                rec.tenant_id.clone(),
                serde_json::json!({
                    "user_id": rec.user_id.clone(),
                    "username": rec.username.clone(),
                    "email": rec.email.clone(),
                    "tenant_id": rec.tenant_id.clone(),
                    "project_id": rec.project_id.clone(),
                    "account_kind": rec.account_kind,
                    "created_by": rec.created_by.clone(),
                }),
            )
            .with_correlation(format!("create_user:{user_id}")),
        )
        .await;
        Ok(Response::new(authn_pb::CreateUserResponse {
            user: Some(user_record_to_pb(&rec)),
            otp_id,
        }))
    }
    async fn get_user(
        &self,
        request: Request<authn_pb::GetUserRequest>,
    ) -> Result<Response<authn_pb::GetUserResponse>, Status> {
        let req = request.into_inner();
        let user = if !req.user_id.trim().is_empty() {
            self.users.get_user_by_id(&req.user_id).await
        } else if !req.username.trim().is_empty() {
            self.users
                .get_user_by_username(&req.username.to_ascii_lowercase())
                .await
        } else if !req.email.trim().is_empty() {
            self.users
                .get_user_by_email(&req.email.to_ascii_lowercase())
                .await
        } else {
            return Err(Status::invalid_argument(
                "one of user_id, username, or email is required",
            ));
        }
        .map_err(Status::internal)?
        .ok_or_else(|| Status::not_found("user not found"))?;
        Ok(Response::new(authn_pb::GetUserResponse {
            user: Some(user_record_to_pb(&user)),
        }))
    }
    async fn list_users(
        &self,
        request: Request<authn_pb::ListUsersRequest>,
    ) -> Result<Response<authn_pb::ListUsersResponse>, Status> {
        let req = request.into_inner();
        let page = req.page.as_ref();
        let (limit, offset, _) = bounded_page_window(page);
        let (users, total) = self
            .users
            .list_users_page(&req.tenant_id, req.account_kind, req.status, limit, offset)
            .await
            .map_err(Status::internal)?;
        let users = users.iter().map(user_record_to_pb).collect();
        Ok(Response::new(authn_pb::ListUsersResponse {
            users,
            page: Some(bounded_page_response(total, page)),
        }))
    }
    async fn update_user(
        &self,
        request: Request<authn_pb::UpdateUserRequest>,
    ) -> Result<Response<authn_pb::UpdateUserResponse>, Status> {
        let req = request.into_inner();
        let mut rec = self
            .users
            .get_user_by_id(&req.user_id)
            .await
            .map_err(Status::internal)?
            .ok_or_else(|| Status::not_found("user not found"))?;
        if !req.full_name.trim().is_empty() {
            rec.full_name = req.full_name;
        }
        if !req.email.trim().is_empty() {
            rec.email = req.email.trim().to_ascii_lowercase();
        }
        if !req.tenant_id.trim().is_empty() {
            rec.tenant_id = req.tenant_id;
        }
        if req.account_kind != authn_entity_pb::AccountKind::Unspecified as i32 {
            rec.account_kind = req.account_kind;
        }
        if !req.project_id.trim().is_empty() {
            rec.project_id = req.project_id;
        }
        if !req.external_provider_id.trim().is_empty() {
            rec.external_provider_id = req.external_provider_id;
        }
        if !req.external_subject.trim().is_empty() {
            rec.external_subject = req.external_subject;
        }
        if !req.profile_attributes.is_empty() {
            rec.profile_attributes_json =
                serde_json::to_string(&req.profile_attributes).unwrap_or_else(|_| "{}".to_string());
        }
        rec.updated_at_unix = now_unix();
        self.users
            .put_user(rec.clone())
            .await
            .map_err(Status::internal)?;
        Ok(Response::new(authn_pb::UpdateUserResponse {
            user: Some(user_record_to_pb(&rec)),
        }))
    }
    async fn change_user_status(
        &self,
        request: Request<authn_pb::ChangeUserStatusRequest>,
    ) -> Result<Response<authn_pb::ChangeUserStatusResponse>, Status> {
        let req = request.into_inner();
        if req.new_status == authn_entity_pb::UserStatus::Unspecified as i32 {
            return Err(Status::invalid_argument("new_status is required"));
        }
        let mut rec = self
            .users
            .get_user_by_id(&req.user_id)
            .await
            .map_err(Status::internal)?
            .ok_or_else(|| Status::not_found("user not found"))?;
        let old_status = rec.status;
        rec.status = req.new_status;
        rec.updated_at_unix = now_unix();
        self.users
            .put_user(rec.clone())
            .await
            .map_err(Status::internal)?;
        self.emit_event(AuthEvent::new(
            topics::USER_STATUS_CHANGED,
            rec.user_id.clone(),
            rec.tenant_id.clone(),
            serde_json::json!({
                "user_id": rec.user_id.clone(),
                "old_status": old_status,
                "new_status": req.new_status,
                "reason": req.reason.clone(),
                "tenant_id": rec.tenant_id.clone(),
            }),
        ))
        .await;
        Ok(Response::new(authn_pb::ChangeUserStatusResponse {
            user: Some(user_record_to_pb(&rec)),
        }))
    }
    async fn admin_reset_password(
        &self,
        request: Request<authn_pb::AdminResetPasswordRequest>,
    ) -> Result<Response<authn_pb::AdminResetPasswordResponse>, Status> {
        let req = request.into_inner();
        let user = self
            .users
            .get_user_by_id(&req.user_id)
            .await
            .map_err(Status::internal)?
            .ok_or_else(|| Status::not_found("user not found"))?;
        let (otp_id, _code) = self
            .issue_otp(
                &user,
                authn_entity_pb::OtpType::PasswordReset as i32,
                format!("admin_reset_password:{}", user.user_id),
                now_unix(),
            )
            .await?;
        Ok(Response::new(authn_pb::AdminResetPasswordResponse {
            otp_id,
        }))
    }
    async fn send_otp(
        &self,
        request: Request<authn_pb::SendOtpRequest>,
    ) -> Result<Response<authn_pb::SendOtpResponse>, Status> {
        let req = request.into_inner();
        let user = self
            .users
            .get_user_by_id(&req.user_id)
            .await
            .map_err(Status::internal)?
            .ok_or_else(|| Status::not_found("user not found"))?;
        let otp_type = if req.otp_type == authn_entity_pb::OtpType::Unspecified as i32 {
            authn_entity_pb::OtpType::SensitiveOperation as i32
        } else {
            req.otp_type
        };
        let (otp_id, _code) = self
            .issue_otp(&user, otp_type, req.correlation_id, now_unix())
            .await?;
        self.emit_event(AuthEvent::new(
            topics::OTP_SENT,
            otp_id.clone(),
            user.tenant_id.clone(),
            serde_json::json!({
                "otp_id": otp_id.clone(),
                "user_id": user.user_id.clone(),
                "otp_type": otp_type,
                "tenant_id": user.tenant_id.clone(),
            }),
        ))
        .await;
        Ok(Response::new(authn_pb::SendOtpResponse {
            otp_id,
            expires_in_seconds: self.config.otp_ttl_secs as i32,
            cooldown_seconds: self.config.otp_cooldown_secs as i32,
        }))
    }
    async fn verify_otp(
        &self,
        request: Request<authn_pb::VerifyOtpRequest>,
    ) -> Result<Response<authn_pb::VerifyOtpResponse>, Status> {
        let req = request.into_inner();
        let verified = self
            .verify_otp_record(&req.otp_id, &req.code, None, now_unix())
            .await?;
        if let Some(rec) = verified {
            if rec.otp_type == authn_entity_pb::OtpType::EmailVerification as i32 {
                if let Some(mut user) = self
                    .users
                    .get_user_by_id(&rec.user_id)
                    .await
                    .map_err(Status::internal)?
                {
                    user.status = authn_entity_pb::UserStatus::Active as i32;
                    user.email_verified_at_unix = now_unix();
                    user.updated_at_unix = now_unix();
                    let event_email = user.email.clone();
                    let event_tenant = user.tenant_id.clone();
                    self.users.put_user(user).await.map_err(Status::internal)?;
                    self.emit_event(AuthEvent::new(
                        topics::EMAIL_VERIFIED,
                        rec.user_id.clone(),
                        event_tenant,
                        serde_json::json!({
                            "user_id": rec.user_id.clone(),
                            "email": event_email,
                        }),
                    ))
                    .await;
                }
            }
            Ok(Response::new(authn_pb::VerifyOtpResponse {
                verified: true,
                user_id: rec.user_id,
                otp_type: rec.otp_type,
            }))
        } else {
            Ok(Response::new(authn_pb::VerifyOtpResponse {
                verified: false,
                user_id: String::new(),
                otp_type: authn_entity_pb::OtpType::Unspecified as i32,
            }))
        }
    }
    async fn resend_otp(
        &self,
        request: Request<authn_pb::ResendOtpRequest>,
    ) -> Result<Response<authn_pb::ResendOtpResponse>, Status> {
        let req = request.into_inner();
        let mut original = self
            .users
            .get_otp(&req.original_otp_id)
            .await
            .map_err(Status::internal)?
            .ok_or_else(|| Status::not_found("otp not found"))?;
        let user = self
            .users
            .get_user_by_id(&original.user_id)
            .await
            .map_err(Status::internal)?
            .ok_or_else(|| Status::not_found("user not found"))?;
        let now = now_unix();
        let (otp_id, _code) = self
            .issue_otp(&user, original.otp_type, req.reason, now)
            .await?;
        original.status = authn_entity_pb::OtpStatus::Invalidated as i32;
        original.superseded_by_id = otp_id.clone();
        self.users
            .update_otp(original.clone())
            .await
            .map_err(Status::internal)?;
        Ok(Response::new(authn_pb::ResendOtpResponse {
            otp_id,
            expires_in_seconds: self.config.otp_ttl_secs as i32,
            cooldown_seconds: self.config.otp_cooldown_secs as i32,
            attempts_remaining: (5 - original.attempt_count).max(0),
        }))
    }
    async fn login(
        &self,
        request: Request<authn_pb::LoginRequest>,
    ) -> Result<Response<authn_pb::LoginResponse>, Status> {
        let req = request.into_inner();
        let now = now_unix();
        let user = if !req.mfa_otp_id.trim().is_empty() {
            let rec = self
                .verify_otp_record(
                    &req.mfa_otp_id,
                    &req.totp_code,
                    Some(authn_entity_pb::OtpType::Login2fa as i32),
                    now,
                )
                .await?
                .ok_or_else(|| Status::unauthenticated("invalid MFA code"))?;
            self.users
                .get_user_by_id(&rec.user_id)
                .await
                .map_err(Status::internal)?
                .ok_or_else(|| Status::not_found("user not found"))?
        } else {
            let login_name = req.username.to_ascii_lowercase();
            let mut user = match self
                .users
                .get_user_by_username(&login_name)
                .await
                .map_err(Status::internal)?
            {
                Some(user) => user,
                None => self
                    .users
                    .get_user_by_email(&login_name)
                    .await
                    .map_err(Status::internal)?
                    .ok_or_else(|| Status::unauthenticated("invalid username or password"))?,
            };
            // Brute-force lockout: reject while locked, using the same opaque
            // error as a bad password so the lock state is not an enumeration
            // oracle.
            const MAX_FAILED_LOGINS: i32 = 5;
            const LOCKOUT_SECONDS: u64 = 15 * 60;
            if user.locked_until_unix > now {
                return Err(Status::unauthenticated("invalid username or password"));
            }
            // Verify the password BEFORE revealing account status so a caller
            // without the password cannot distinguish unknown / inactive / locked
            // accounts (all wrong-credential paths return the same error).
            if !authn::verify_password(
                &req.password,
                &self.password_hash_key(),
                &user.password_hash,
            ) {
                user.failed_login_count += 1;
                let now_locked = user.failed_login_count >= MAX_FAILED_LOGINS;
                let attempt_count = user.failed_login_count;
                if now_locked {
                    user.locked_until_unix = now + LOCKOUT_SECONDS;
                    user.failed_login_count = 0;
                }
                user.updated_at_unix = now;
                let locked_until_unix = user.locked_until_unix;
                let event_user_id = user.user_id.clone();
                let event_tenant = user.tenant_id.clone();
                let event_ip = req.ip_address.clone();
                self.users.put_user(user).await.map_err(Status::internal)?;
                if now_locked {
                    self.emit_event(AuthEvent::new(
                        topics::USER_LOCKED,
                        event_user_id.clone(),
                        event_tenant,
                        serde_json::json!({
                            "user_id": event_user_id,
                            "attempt_count": attempt_count,
                            "ip_address": event_ip,
                            "locked_until_unix": locked_until_unix,
                        }),
                    ))
                    .await;
                }
                return Err(Status::unauthenticated("invalid username or password"));
            }
            // Password proven — now it is safe to surface account status.
            if user.status != authn_entity_pb::UserStatus::Active as i32
                && user.status != authn_entity_pb::UserStatus::PendingVerification as i32
            {
                return Err(Status::permission_denied("user is not active"));
            }
            user.failed_login_count = 0;
            user.locked_until_unix = 0;
            user.last_login_at_unix = now;
            user.updated_at_unix = now;
            // Transparently re-hash legacy keyed-HMAC passwords with Argon2id on
            // the first successful login after the KDF upgrade.
            if authn::password_hash_needs_upgrade(&user.password_hash) {
                user.password_hash = authn::hash_password(&req.password, &self.password_hash_key());
            }
            self.users
                .put_user(user.clone())
                .await
                .map_err(Status::internal)?;
            if user.mfa_enabled {
                // TOTP second factor: the client re-submits username/password
                // plus the current authenticator code. With no code yet, signal
                // that MFA is required; with a code, verify it against the
                // user's enrolled secret before issuing a session.
                if req.totp_code.trim().is_empty() {
                    return Ok(Response::new(authn_pb::LoginResponse {
                        user_id: user.user_id,
                        mfa_required: true,
                        ..Default::default()
                    }));
                }
                let verified =
                    authn::totp::decrypt_secret(&user.totp_secret_hash, &self.otp_hash_key())
                        .map(|secret| authn::totp::verify(&secret, &req.totp_code, now))
                        .unwrap_or(false);
                if !verified {
                    return Err(Status::unauthenticated("invalid MFA code"));
                }
            }
            user
        };
        let (session_id, _expires) = self
            .create_login_session(
                &user,
                format!("{}|{}|{}", req.device_name, req.ip_address, req.user_agent),
                now,
            )
            .await?;
        self.emit_event(
            AuthEvent::new(
                topics::USER_LOGGED_IN,
                user.user_id.clone(),
                user.tenant_id.clone(),
                serde_json::json!({
                    "user_id": user.user_id.clone(),
                    "session_id": session_id.clone(),
                    "tenant_id": user.tenant_id.clone(),
                    "project_id": user.project_id.clone(),
                    "device_name": req.device_name.clone(),
                    "ip_address": req.ip_address.clone(),
                }),
            )
            .with_correlation(format!("login:{}", user.user_id)),
        )
        .await;
        // Issue a short-lived signed access token when JWT signing is configured;
        // the server-side session id doubles as the refresh credential.
        let (access_token, access_exp) = self.issue_access_token(
            &user.user_id,
            &user.tenant_id,
            &user.project_id,
            &[],
            &[],
            "",
            &session_id,
            now,
        );
        let access_token_expires_in = if access_exp > 0 {
            (access_exp - now as i64).max(0) as i32
        } else {
            self.config.session_ttl_secs as i32
        };
        let refresh_token = if access_exp > 0 {
            session_id.clone()
        } else {
            String::new()
        };
        // CSRF token for the double-submit cookie pattern (WEB flows); bound to
        // the session and validated by `ValidateCSRF`.
        let csrf_token = self.csrf_token_for(&session_id);
        Ok(Response::new(authn_pb::LoginResponse {
            user_id: user.user_id,
            session_id: session_id.clone(),
            session_token: session_id,
            access_token,
            refresh_token,
            csrf_token,
            access_token_expires_in,
            ..Default::default()
        }))
    }
    async fn refresh_token(
        &self,
        request: Request<authn_pb::RefreshTokenRequest>,
    ) -> Result<Response<authn_pb::RefreshTokenResponse>, Status> {
        let req = request.into_inner();
        let now = now_unix();
        // The refresh credential is a server-side session id (returned as
        // `refresh_token` at login). Accept it from either field for
        // compatibility with session-only callers.
        let session_ref = if !req.refresh_token.trim().is_empty() {
            req.refresh_token.clone()
        } else {
            req.session_id.clone()
        };
        if session_ref.trim().is_empty() {
            return Err(Status::invalid_argument(
                "refresh_token or session_id is required",
            ));
        }
        // Sliding refresh: extend the backing session, then mint a fresh
        // short-lived access token bound to it.
        let Some(rec) = authn::refresh_session(
            self.sessions.as_ref(),
            &session_ref,
            &self.hash_key(),
            now,
            self.config.session_ttl_secs,
        )
        .await
        .map_err(Status::internal)?
        else {
            return Err(Status::unauthenticated("invalid or expired session"));
        };
        let (access_token, access_exp) = self.issue_access_token(
            &rec.user_id,
            &rec.tenant_id,
            &rec.project_id,
            &rec.scopes,
            &rec.roles,
            &rec.service_identity,
            &session_ref,
            now,
        );
        let access_token_expires_in = if access_exp > 0 {
            (access_exp - now as i64).max(0) as i32
        } else {
            self.config.session_ttl_secs as i32
        };
        Ok(Response::new(authn_pb::RefreshTokenResponse {
            access_token,
            access_token_expires_in,
        }))
    }
    async fn logout(
        &self,
        request: Request<authn_pb::LogoutRequest>,
    ) -> Result<Response<authn_pb::LogoutResponse>, Status> {
        let req = request.into_inner();
        let now = now_unix();
        let count = if req.all_sessions {
            let principal_id = req
                .context
                .as_ref()
                .map(|ctx| ctx.principal_id.clone())
                .unwrap_or_default();
            if principal_id.trim().is_empty() {
                return Err(Status::invalid_argument(
                    "context.principal_id is required for all_sessions logout",
                ));
            }
            self.sessions
                .revoke_all_for_principal(&principal_id, now)
                .await
                .map_err(Status::internal)? as i32
        } else {
            let hash = authn::hash_secret(&req.session_id, &self.hash_key());
            i32::from(
                self.sessions
                    .revoke(&hash, now)
                    .await
                    .map_err(Status::internal)?,
            )
        };
        Ok(Response::new(authn_pb::LogoutResponse {
            sessions_revoked: count,
        }))
    }
    async fn change_password(
        &self,
        request: Request<authn_pb::ChangePasswordRequest>,
    ) -> Result<Response<authn_pb::ChangePasswordResponse>, Status> {
        let req = request.into_inner();
        if req.new_password.len() < 10 {
            return Err(Status::invalid_argument(
                "new_password must be at least 10 characters",
            ));
        }
        let mut user = self
            .users
            .get_user_by_id(&req.user_id)
            .await
            .map_err(Status::internal)?
            .ok_or_else(|| Status::not_found("user not found"))?;
        if !authn::verify_password(
            &req.current_password,
            &self.password_hash_key(),
            &user.password_hash,
        ) {
            return Err(Status::unauthenticated("invalid current password"));
        }
        if !req.otp_id.trim().is_empty() {
            let otp = self
                .users
                .get_otp(&req.otp_id)
                .await
                .map_err(Status::internal)?
                .ok_or_else(|| Status::not_found("otp not found"))?;
            // The OTP must belong to the user, be consumed, AND be of a
            // sensitive-operation / password-reset type — otherwise a consumed
            // login-2FA or email-verification OTP would satisfy the gate.
            let type_ok = otp.otp_type == authn_entity_pb::OtpType::PasswordReset as i32
                || otp.otp_type == authn_entity_pb::OtpType::SensitiveOperation as i32;
            if otp.user_id != user.user_id
                || otp.status != authn_entity_pb::OtpStatus::Used as i32
                || !type_ok
            {
                return Err(Status::permission_denied(
                    "password-change OTP is not verified",
                ));
            }
        }
        let now = now_unix();
        let event_tenant = user.tenant_id.clone();
        user.password_hash = authn::hash_password(&req.new_password, &self.password_hash_key());
        user.updated_at_unix = now;
        self.users.put_user(user).await.map_err(Status::internal)?;
        self.emit_event(AuthEvent::new(
            topics::PASSWORD_CHANGED,
            req.user_id.clone(),
            event_tenant,
            serde_json::json!({
                "user_id": req.user_id.clone(),
                "is_reset": false,
                "changed_by": req.user_id.clone(),
            }),
        ))
        .await;
        Ok(Response::new(authn_pb::ChangePasswordResponse {
            user_id: req.user_id,
            changed_at: timestamp_from_unix(now),
            operation_id: Uuid::new_v4().to_string(),
        }))
    }
    async fn validate_token(
        &self,
        request: Request<authn_pb::ValidateTokenRequest>,
    ) -> Result<Response<authn_pb::ValidateTokenResponse>, Status> {
        let req = request.into_inner();
        let now = now_unix();
        let token_type = authn_entity_pb::TokenType::try_from(req.token_type).unwrap_or_default();
        let response = match token_type {
            authn_entity_pb::TokenType::Session => {
                let rec = authn::validate_session(
                    self.sessions.as_ref(),
                    &req.token,
                    &self.hash_key(),
                    now,
                    self.config.session_idle_ttl_secs,
                )
                .await
                .map_err(Status::internal)?;
                validate_session_response(rec, &req.token, now)
            }
            authn_entity_pb::TokenType::ApiKey => {
                let rec = authn::validate_api_key(
                    self.api_keys.as_ref(),
                    &req.token,
                    &self.api_key_hash_key(),
                    now,
                )
                .await
                .map_err(Status::internal)?;
                validate_api_key_response(rec)
            }
            authn_entity_pb::TokenType::JwtAccess | authn_entity_pb::TokenType::JwtRefresh => {
                let claims = validate_bearer_token(&self.security, &req.token)
                    .map_err(Status::unauthenticated)?;
                let subject = claims.sub.clone().unwrap_or_default();
                let principal = Principal {
                    principal_id: subject.clone(),
                    subject: subject.clone(),
                    user_id: subject.clone(),
                    service_identity: claims.service_identity.clone().unwrap_or_default(),
                    tenant_id: claims.tenant_id.clone().unwrap_or_default(),
                    project_id: claims.project_id.clone().unwrap_or_default(),
                    scopes: claims.resolved_scopes(),
                    roles: claims.roles.clone().unwrap_or_default(),
                    provider_id: String::new(),
                    auth_method: authn::AuthnMethod::Jwt.as_str().to_string(),
                };
                authn_pb::ValidateTokenResponse {
                    valid: true,
                    user_id: subject,
                    session_id: String::new(),
                    account_kind: authn_entity_pb::AccountKind::Unspecified as i32,
                    tenant_id: principal.tenant_id.clone(),
                    roles: principal.roles.clone(),
                    expires_at: None,
                    access_surface: "jwt".to_string(),
                    device_id: String::new(),
                    token_id: claims.jti.clone().unwrap_or_default(),
                    session_type: authn_entity_pb::SessionType::Jwt as i32,
                    principal: Some(authn_principal_to_pb(&principal, 0)),
                    project_id: principal.project_id.clone(),
                    scopes: principal.scopes.clone(),
                    attributes: Default::default(),
                }
            }
            _ => {
                return Err(Status::invalid_argument(
                    "supported token_type values are SESSION, API_KEY, JWT_ACCESS, and JWT_REFRESH",
                ));
            }
        };
        Ok(Response::new(response))
    }
    async fn get_session(
        &self,
        request: Request<authn_pb::GetSessionRequest>,
    ) -> Result<Response<authn_pb::GetSessionResponse>, Status> {
        let req = request.into_inner();
        let hash = authn::hash_secret(&req.session_id, &self.hash_key());
        let now = now_unix();
        let session = self
            .sessions
            .get(&hash)
            .await
            .map_err(Status::internal)?
            .map(|rec| session_record_to_pb(&rec, now));
        Ok(Response::new(authn_pb::GetSessionResponse { session }))
    }
    async fn list_sessions(
        &self,
        request: Request<authn_pb::ListSessionsRequest>,
    ) -> Result<Response<authn_pb::ListSessionsResponse>, Status> {
        let req = request.into_inner();
        if req.user_id.trim().is_empty() {
            return Err(Status::invalid_argument("user_id is required"));
        }
        let now = now_unix();
        let page = req.page.as_ref();
        let (limit, offset, _) = bounded_page_window(page);
        let (sessions, total) = self
            .sessions
            .list_for_principal_page(&req.user_id, req.active_only, now, limit, offset)
            .await
            .map_err(Status::internal)?;
        let sessions = sessions
            .iter()
            .map(|rec| session_record_to_pb(rec, now))
            .collect();
        Ok(Response::new(authn_pb::ListSessionsResponse {
            sessions,
            page: Some(bounded_page_response(total, page)),
        }))
    }
    async fn validate_csrf(
        &self,
        request: Request<authn_pb::ValidateCsrfRequest>,
    ) -> Result<Response<authn_pb::ValidateCsrfResponse>, Status> {
        let req = request.into_inner();
        if req.session_id.trim().is_empty() || req.csrf_token.trim().is_empty() {
            return Ok(Response::new(authn_pb::ValidateCsrfResponse {
                valid: false,
            }));
        }
        let now = now_unix();
        // Signed double-submit cookie pattern: the CSRF token is a keyed HMAC
        // bound to the session id (issued at login). Validate the session is
        // live, then constant-time compare the presented token to the expected
        // binding — a forged token can't be produced without the server secret.
        let session_live = authn::validate_session(
            self.sessions.as_ref(),
            &req.session_id,
            &self.hash_key(),
            now,
            self.config.session_idle_ttl_secs,
        )
        .await
        .map_err(Status::internal)?
        .is_some();
        let expected = self.csrf_token_for(&req.session_id);
        let token_ok = authn::constant_time_eq(&expected, &req.csrf_token);
        Ok(Response::new(authn_pb::ValidateCsrfResponse {
            valid: session_live && token_ok,
        }))
    }
    async fn enroll_mfa(
        &self,
        request: Request<authn_pb::EnrollMfaRequest>,
    ) -> Result<Response<authn_pb::EnrollMfaResponse>, Status> {
        let req = request.into_inner();
        if req.mfa_type == authn_entity_pb::AuthFactorKind::Webauthn as i32 {
            return Err(Status::failed_precondition(
                "WebAuthn enrollment uses StartWebAuthnRegistration and FinishWebAuthnRegistration",
            ));
        }
        let mut user = self
            .users
            .get_user_by_id(&req.user_id)
            .await
            .map_err(Status::internal)?
            .ok_or_else(|| Status::not_found("user not found"))?;
        let now = now_unix();
        // RFC 6238 TOTP: mint a real base32 secret, store it encrypted-at-rest
        // as a *pending* enrollment (MFA stays disabled until the user proves
        // possession with a code), and hand the secret + provisioning URI back
        // once for the authenticator app to scan.
        let secret = authn::totp::generate_secret();
        let enc = authn::totp::encrypt_secret(&secret, &self.otp_hash_key())
            .ok_or_else(|| Status::internal("failed to secure TOTP secret"))?;
        let uri = authn::totp::provisioning_uri("UDB", &user.username, &secret);
        user.totp_secret_hash = enc;
        user.updated_at_unix = now;
        self.users.put_user(user).await.map_err(Status::internal)?;
        Ok(Response::new(authn_pb::EnrollMfaResponse {
            totp_secret: secret,
            totp_qr_uri: uri,
            // TOTP enrollment is confirmed by presenting a code, not a server
            // OTP id; the field stays empty for the TOTP factor.
            verify_otp_id: String::new(),
        }))
    }
    async fn confirm_mfa_enrollment(
        &self,
        request: Request<authn_pb::ConfirmMfaEnrollmentRequest>,
    ) -> Result<Response<authn_pb::ConfirmMfaEnrollmentResponse>, Status> {
        let req = request.into_inner();
        let now = now_unix();
        let mut user = self
            .users
            .get_user_by_id(&req.user_id)
            .await
            .map_err(Status::internal)?
            .ok_or_else(|| Status::not_found("user not found"))?;
        // Verify the submitted code against the pending TOTP secret minted at
        // enrollment. Only flip `mfa_enabled` on a valid code.
        let verified = authn::totp::decrypt_secret(&user.totp_secret_hash, &self.otp_hash_key())
            .map(|secret| authn::totp::verify(&secret, &req.code, now))
            .unwrap_or(false);
        if !verified {
            return Ok(Response::new(authn_pb::ConfirmMfaEnrollmentResponse {
                enrolled: false,
            }));
        }
        user.mfa_enabled = true;
        user.updated_at_unix = now;
        self.users.put_user(user).await.map_err(Status::internal)?;
        Ok(Response::new(authn_pb::ConfirmMfaEnrollmentResponse {
            enrolled: true,
        }))
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
}
