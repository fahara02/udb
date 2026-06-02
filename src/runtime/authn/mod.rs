//! Stage 1 UDB-owned authentication primitives (library-free).
//!
//! This module owns the authn *interfaces* and the storage-independent logic
//! for server-side sessions, API/service keys, and hybrid external identities.
//! Session ids and API keys are never stored raw — only keyed HMAC-SHA256
//! digests; passwords use Argon2id ([`hash_password`]); TOTP secrets are
//! encrypted at rest ([`totp`]). Persistence is exclusively the Postgres-backed
//! stores (fail-closed when the pool is absent) — there are no in-memory stores.

use async_trait::async_trait;
#[cfg(feature = "redis")]
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row};

use crate::runtime::security::hmac_sha256;

use crate::proto::udb::core::apikey::entity::v1 as apikey_entity_pb;
use crate::proto::udb::core::authn::entity::v1 as authn_entity_pb;
use crate::runtime::authz::Principal;
use crate::runtime::native_catalog::{NativeModel, native_model};

/// RFC 6238 TOTP for native MFA (enrollment, verification, secret-at-rest).
pub mod totp;

/// How a principal authenticated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AuthnMethod {
    #[default]
    Unknown,
    Jwt,
    Session,
    ApiKey,
    Mtls,
    /// Dev-only header fallback (must be denied in production).
    HeaderDev,
    /// Hybrid external identity provider (OIDC/Better Auth/etc.).
    External,
    /// WebAuthn/passkey assertion verified by UDB.
    WebAuthn,
}

impl AuthnMethod {
    pub fn as_str(&self) -> &'static str {
        match self {
            AuthnMethod::Unknown => "unknown",
            AuthnMethod::Jwt => "jwt",
            AuthnMethod::Session => "session",
            AuthnMethod::ApiKey => "api_key",
            AuthnMethod::Mtls => "mtls",
            AuthnMethod::HeaderDev => "header_dev",
            AuthnMethod::External => "external",
            AuthnMethod::WebAuthn => "webauthn",
        }
    }
}

/// The result of authenticating a caller.
#[derive(Debug, Clone, Default)]
pub struct AuthnOutcome {
    pub principal: Principal,
    pub method: AuthnMethod,
    /// Opaque session id issued/used, if any (the raw value — callers must hash
    /// before persisting).
    pub session_id: String,
    pub expires_at_unix: u64,
    pub relationship_version: String,
    pub warnings: Vec<String>,
}

/// Keyed HMAC-SHA256 of a secret, hex-encoded with a `hmac-sha256:` tag.
///
/// Used to derive the stored form of session ids and API keys so a leaked
/// database cannot reverse them to usable credentials. `key` is the deployment
/// `UDB_SESSION_HASH_SECRET`. Same construction as `plan_approval::compute_signature`.
pub fn hash_secret(secret: &str, key: &[u8]) -> String {
    let mac = hmac_sha256(key, secret.as_bytes());
    let mut out = String::with_capacity("hmac-sha256:".len() + mac.len() * 2);
    out.push_str("hmac-sha256:");
    for b in mac {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

/// A short, non-secret lookup prefix for an API key (the part before the dot in
/// `udbk_<prefix>.<secret>`), used to index the row without exposing the secret.
pub fn api_key_prefix(raw_key: &str) -> String {
    raw_key
        .split_once('.')
        .map(|(prefix, _)| prefix.to_string())
        .unwrap_or_else(|| raw_key.chars().take(12).collect())
}

/// Session / API-key authn configuration, read from the environment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthnConfig {
    /// `UDB_SESSION_ENABLED` — server-side sessions on/off (default off).
    pub session_enabled: bool,
    /// `UDB_SESSION_BACKEND` — `postgres` (default) or `redis`.
    pub session_backend: String,
    /// `UDB_SESSION_TTL_SECONDS` — absolute session lifetime (default 86400).
    pub session_ttl_secs: u64,
    /// `UDB_SESSION_IDLE_TTL_SECONDS` — idle timeout (default 3600).
    pub session_idle_ttl_secs: u64,
    /// `UDB_SESSION_HEADER` — metadata header carrying the session id.
    pub session_header: String,
    /// `UDB_SESSION_COOKIE` — cookie name carrying the session id.
    pub session_cookie: String,
    /// `UDB_SESSION_HASH_SECRET` — keyed-HMAC secret for `hash_secret`. Empty
    /// disables persistence (sessions cannot be stored without a hash key).
    pub session_hash_secret: String,
    /// `UDB_API_KEY_HASH_SECRET` — keyed-HMAC secret for API keys. Falls back
    /// to `UDB_SESSION_HASH_SECRET` during Stage 1 when unset.
    pub api_key_hash_secret: String,
    /// `UDB_PASSWORD_HASH_SECRET` — keyed-HMAC secret for native passwords.
    /// Falls back to `UDB_SESSION_HASH_SECRET` during Stage 1 when unset.
    pub password_hash_secret: String,
    /// `UDB_OTP_HASH_SECRET` — keyed-HMAC secret for one-time codes. Falls back
    /// to `UDB_SESSION_HASH_SECRET` during Stage 1 when unset.
    pub otp_hash_secret: String,
    /// `UDB_OTP_TTL_SECONDS` — OTP lifetime (default 600; long enough for
    /// email/SMS delivery jitter, short enough to limit replay value).
    pub otp_ttl_secs: u64,
    /// `UDB_OTP_COOLDOWN_SECONDS` — resend cooldown hint (default 60).
    pub otp_cooldown_secs: u64,
}

impl Default for AuthnConfig {
    fn default() -> Self {
        Self {
            session_enabled: false,
            session_backend: "postgres".to_string(),
            // Defaults favor a normal workday login: 24h absolute lifetime with
            // a 1h idle window. Operators can shorten both for high-risk apps.
            session_ttl_secs: 86_400,
            session_idle_ttl_secs: 3_600,
            session_header: "x-udb-session".to_string(),
            session_cookie: "udb_session".to_string(),
            session_hash_secret: String::new(),
            api_key_hash_secret: String::new(),
            password_hash_secret: String::new(),
            otp_hash_secret: String::new(),
            // OTP defaults: 10m validity and 60s resend cooldown balance
            // delivery latency against brute-force/replay exposure.
            otp_ttl_secs: 600,
            otp_cooldown_secs: 60,
        }
    }
}

impl AuthnConfig {
    pub fn from_env() -> Self {
        let d = Self::default();
        let truthy = |key: &str, default: bool| {
            std::env::var(key)
                .map(|v| !matches!(v.as_str(), "0" | "false" | "no" | "off"))
                .unwrap_or(default)
        };
        let u64_env = |key: &str, default: u64| {
            std::env::var(key)
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(default)
        };
        let str_env = |key: &str, default: String| std::env::var(key).unwrap_or(default);
        Self {
            session_enabled: truthy("UDB_SESSION_ENABLED", d.session_enabled),
            session_backend: str_env("UDB_SESSION_BACKEND", d.session_backend),
            session_ttl_secs: u64_env("UDB_SESSION_TTL_SECONDS", d.session_ttl_secs),
            session_idle_ttl_secs: u64_env("UDB_SESSION_IDLE_TTL_SECONDS", d.session_idle_ttl_secs),
            session_header: str_env("UDB_SESSION_HEADER", d.session_header),
            session_cookie: str_env("UDB_SESSION_COOKIE", d.session_cookie),
            session_hash_secret: std::env::var("UDB_SESSION_HASH_SECRET").unwrap_or_default(),
            api_key_hash_secret: std::env::var("UDB_API_KEY_HASH_SECRET").unwrap_or_default(),
            password_hash_secret: std::env::var("UDB_PASSWORD_HASH_SECRET").unwrap_or_default(),
            otp_hash_secret: std::env::var("UDB_OTP_HASH_SECRET").unwrap_or_default(),
            otp_ttl_secs: u64_env("UDB_OTP_TTL_SECONDS", d.otp_ttl_secs),
            otp_cooldown_secs: u64_env("UDB_OTP_COOLDOWN_SECONDS", d.otp_cooldown_secs),
        }
    }

    /// Sessions can only be persisted when enabled AND a hash secret is set
    /// (raw session ids must never be stored).
    pub fn sessions_usable(&self) -> bool {
        self.session_enabled && !self.session_hash_secret.trim().is_empty()
    }

    pub fn api_key_hash_secret(&self) -> &str {
        if self.api_key_hash_secret.trim().is_empty() {
            &self.session_hash_secret
        } else {
            &self.api_key_hash_secret
        }
    }

    pub fn password_hash_secret(&self) -> &str {
        if self.password_hash_secret.trim().is_empty() {
            &self.session_hash_secret
        } else {
            &self.password_hash_secret
        }
    }

    pub fn otp_hash_secret(&self) -> &str {
        if self.otp_hash_secret.trim().is_empty() {
            &self.session_hash_secret
        } else {
            &self.otp_hash_secret
        }
    }
}

/// A server-side session record. `*_unix` are seconds; `revoked_at_unix == 0`
/// means active.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionRecord {
    pub session_id_hash: String,
    pub principal_id: String,
    pub user_id: String,
    pub service_identity: String,
    pub tenant_id: String,
    pub project_id: String,
    pub scopes: Vec<String>,
    pub roles: Vec<String>,
    pub relationship_version: String,
    pub created_at_unix: u64,
    pub updated_at_unix: u64,
    pub expires_at_unix: u64,
    pub revoked_at_unix: u64,
    pub client_fingerprint: String,
}

impl SessionRecord {
    pub fn is_revoked(&self) -> bool {
        self.revoked_at_unix != 0
    }
    pub fn is_expired(&self, now_unix: u64) -> bool {
        self.expires_at_unix != 0 && now_unix >= self.expires_at_unix
    }
    pub fn is_active(&self, now_unix: u64) -> bool {
        // #202 REVERTED: the `created_at_unix <= now_unix` guard rejected
        // legitimately-validated sessions whenever the validation clock was at or
        // behind the creation instant — i.e. real clock skew (the Postgres
        // container's `NOW()` running ahead of the validating host) and the
        // logical timelines the live tests use. `created_at` is server-set, so a
        // "future" value cannot be forged without the session hash secret; the
        // guard's security value did not justify rejecting valid sessions.
        !self.is_revoked() && !self.is_expired(now_unix)
    }
    pub fn is_idle_expired(&self, now_unix: u64, idle_ttl_secs: u64) -> bool {
        idle_ttl_secs != 0
            && self.updated_at_unix != 0
            && now_unix.saturating_sub(self.updated_at_unix) > idle_ttl_secs
    }
    pub fn is_active_with_idle(&self, now_unix: u64, idle_ttl_secs: u64) -> bool {
        self.is_active(now_unix) && !self.is_idle_expired(now_unix, idle_ttl_secs)
    }
}

/// An API/service key record. The secret is stored only as `key_hash`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ApiKeyRecord {
    pub key_prefix: String,
    pub key_hash: String,
    pub principal_id: String,
    pub service_identity: String,
    pub tenant_id: String,
    pub project_id: String,
    pub scopes: Vec<String>,
    pub created_at_unix: u64,
    pub last_used_at_unix: u64,
    pub expires_at_unix: u64,
    pub revoked_at_unix: u64,
}

impl ApiKeyRecord {
    pub fn is_revoked(&self) -> bool {
        self.revoked_at_unix != 0
    }
    pub fn is_expired(&self, now_unix: u64) -> bool {
        self.expires_at_unix != 0 && now_unix >= self.expires_at_unix
    }
    pub fn is_active(&self, now_unix: u64) -> bool {
        // #202 REVERTED (see SessionRecord::is_active): the `created_at_unix <=
        // now_unix` guard rejected valid keys under clock skew / logical test
        // timelines for negligible security benefit (created_at is server-set).
        !self.is_revoked() && !self.is_expired(now_unix)
    }
}

/// A hybrid external identity mapped to a UDB principal.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IdentityRecord {
    pub provider_id: String,
    pub external_subject: String,
    pub principal_id: String,
    pub tenant_id: String,
    pub project_id: String,
    pub claims_json: String,
    pub created_at_unix: u64,
    pub updated_at_unix: u64,
    pub disabled_at_unix: u64,
}

/// Native user account record stored by Stage-1 authn.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UserRecord {
    pub user_id: String,
    pub username: String,
    pub email: String,
    pub password_hash: String,
    pub account_kind: i32,
    pub status: i32,
    pub tenant_id: String,
    pub full_name: String,
    pub totp_secret_hash: String,
    pub mfa_enabled: bool,
    pub failed_login_count: i32,
    pub locked_until_unix: u64,
    pub email_verified_at_unix: u64,
    pub last_login_at_unix: u64,
    pub created_by: String,
    pub created_at_unix: u64,
    pub updated_at_unix: u64,
    pub deleted_at_unix: u64,
    pub deleted_by: String,
    pub project_id: String,
    pub external_provider_id: String,
    pub external_subject: String,
    pub profile_attributes_json: String,
}

/// One-time password / step-up challenge record.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OtpRecord {
    pub otp_id: String,
    pub user_id: String,
    pub otp_type: i32,
    pub code_hash: String,
    pub delivery_channel: String,
    pub delivery_address: String,
    pub status: i32,
    pub attempt_count: i32,
    pub superseded_by_id: String,
    pub expires_at_unix: u64,
    pub used_at_unix: u64,
    pub created_at_unix: u64,
    pub correlation_id: String,
}

// ── Authn interfaces (Postgres/Redis impls are a thin async layer) ──────────

/// Validates a caller identity into an `AuthnOutcome`. Implemented per method
/// (JWT today via `security.rs`; session/api-key/external added incrementally).
pub trait Authenticator: Send + Sync {
    /// Returns the principal for `token`/`session_id`/`api_key` material, or an
    /// error string. Async so DB-backed validators fit the same trait.
    fn authenticate(&self, outcome_hint: &AuthnMethod) -> Result<AuthnOutcome, String>;
}

/// Hybrid external identity provider: authenticates externally, then UDB maps
/// the verified subject/claims to a `Principal`. UDB still owns authorization.
pub trait IdentityProvider: Send + Sync {
    fn provider_id(&self) -> &str;
    /// Map a verified external subject + claims to a UDB principal.
    fn map_identity(&self, external_subject: &str, claims_json: &str) -> Result<Principal, String>;
}

/// Which claim names a generic external JWT/OIDC provider's token uses for the
/// fields UDB needs. Lets one adapter handle Auth0/Okta/Better-Auth/Cognito/etc.
#[derive(Debug, Clone)]
pub struct ExternalProviderConfig {
    pub provider_id: String,
    pub subject_claim: String,
    pub tenant_claim: String,
    pub project_claim: String,
    pub roles_claim: String,
    pub scopes_claim: String,
}

impl Default for ExternalProviderConfig {
    fn default() -> Self {
        Self {
            provider_id: "external".to_string(),
            subject_claim: "sub".to_string(),
            tenant_claim: "tenant_id".to_string(),
            project_claim: "project_id".to_string(),
            roles_claim: "roles".to_string(),
            scopes_claim: "scopes".to_string(),
        }
    }
}

/// Generic external JWT/OIDC identity provider. The token is assumed already
/// signature/issuer/audience-verified upstream (or by UDB's JWT path); this maps
/// its verified claims to a UDB `Principal`. **It never confers authorization** —
/// the resulting principal still goes through the UDB authz engine, so external
/// roles alone cannot bypass UDB policy.
pub struct ExternalJwtProvider {
    cfg: ExternalProviderConfig,
}

impl ExternalJwtProvider {
    pub fn new(cfg: ExternalProviderConfig) -> Self {
        Self { cfg }
    }
}

/// Extract a claim as a string (string, or number coerced to string).
fn claim_str(claims: &serde_json::Value, key: &str) -> String {
    match claims.get(key) {
        Some(serde_json::Value::String(s)) => s.clone(),
        Some(serde_json::Value::Number(n)) => n.to_string(),
        _ => String::new(),
    }
}

/// Extract a claim as a list of strings (JSON array, or space/comma-separated string).
fn claim_list(claims: &serde_json::Value, key: &str) -> Vec<String> {
    match claims.get(key) {
        Some(serde_json::Value::Array(items)) => items
            .iter()
            .filter_map(|v| v.as_str().map(ToString::to_string))
            .collect(),
        Some(serde_json::Value::String(s)) => s
            .split([' ', ','])
            .map(str::trim)
            .filter(|p| !p.is_empty())
            .map(ToString::to_string)
            .collect(),
        _ => Vec::new(),
    }
}

impl IdentityProvider for ExternalJwtProvider {
    fn provider_id(&self) -> &str {
        &self.cfg.provider_id
    }

    fn map_identity(&self, external_subject: &str, claims_json: &str) -> Result<Principal, String> {
        let claims: serde_json::Value =
            serde_json::from_str(claims_json).map_err(|e| format!("invalid claims json: {e}"))?;
        let subject = {
            let from_claim = claim_str(&claims, &self.cfg.subject_claim);
            if !from_claim.is_empty() {
                from_claim
            } else {
                external_subject.to_string()
            }
        };
        if subject.is_empty() {
            return Err("external token has no subject".to_string());
        }
        Ok(Principal {
            principal_id: format!("{}:{}", self.cfg.provider_id, subject),
            subject: subject.clone(),
            user_id: subject,
            service_identity: String::new(),
            tenant_id: claim_str(&claims, &self.cfg.tenant_claim),
            project_id: claim_str(&claims, &self.cfg.project_claim),
            scopes: claim_list(&claims, &self.cfg.scopes_claim),
            roles: claim_list(&claims, &self.cfg.roles_claim),
            provider_id: self.cfg.provider_id.clone(),
            auth_method: AuthnMethod::External.as_str().to_string(),
        })
    }
}

fn scopes_from_db(raw: &str) -> Vec<String> {
    raw.split([',', ' ', '\n', '\t'])
        .map(str::trim)
        .filter(|scope| !scope.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn string_list_to_json(values: &[String]) -> String {
    serde_json::to_string(values).unwrap_or_else(|_| "[]".to_string())
}

fn string_list_from_json(raw: &str) -> Vec<String> {
    serde_json::from_str::<Vec<String>>(raw).unwrap_or_else(|_| scopes_from_db(raw))
}

fn json_object_or_empty(raw: &str) -> String {
    serde_json::from_str::<serde_json::Value>(raw)
        .ok()
        .filter(|value| value.is_object())
        .map(|value| value.to_string())
        .unwrap_or_else(|| "{}".to_string())
}

fn row_string(row: &sqlx::postgres::PgRow, column: &str) -> Result<String, sqlx::Error> {
    row.try_get::<Option<String>, _>(column)
        .map(|value| value.unwrap_or_default())
}

fn uuid_or_empty_as_text(model: &NativeModel, field_name: &str) -> String {
    model.text_or_empty(field_name)
}

/// Generate the `<name>_to_db` / `<name>_from_db` mapping pair for a proto enum
/// stored as an upper-snake-case DB string.
///
/// - `to_db(i32)` decodes the proto enum (`try_from(..).unwrap_or_default()`) and
///   maps each variant to its canonical DB string; the `unspecified` variant maps
///   to `"UNSPECIFIED"`.
/// - `from_db(&str)` accepts either the bare DB string (`"PERSON"`) or the
///   proto-prefixed form (`"ACCOUNT_KIND_PERSON"`) by stripping the optional
///   `$prefix` before matching, and falls back to `unspecified` for anything else.
///
/// Behavior is identical to the previous hand-written pairs (the strip-prefix
/// normalization accepts exactly the bare + single-prefixed spellings the old
/// `"X" | "PREFIX_X"` arms did).
macro_rules! enum_db_mapping {
    (
        $enum_path:path,
        prefix = $prefix:literal,
        to_db = $to_db:ident,
        from_db = $from_db:ident,
        unspecified = $unspecified:ident,
        $( $variant:ident => $db:literal ),+ $(,)?
    ) => {
        fn $to_db(value: i32) -> &'static str {
            use $enum_path as Enum;
            match Enum::try_from(value).unwrap_or_default() {
                $( Enum::$variant => $db, )+
                Enum::$unspecified => "UNSPECIFIED",
            }
        }

        fn $from_db(value: &str) -> i32 {
            use $enum_path as Enum;
            let bare = value.strip_prefix($prefix).unwrap_or(value);
            match bare {
                $( $db => Enum::$variant as i32, )+
                _ => Enum::$unspecified as i32,
            }
        }
    };
}

enum_db_mapping! {
    authn_entity_pb::AccountKind,
    prefix = "ACCOUNT_KIND_",
    to_db = account_kind_to_db,
    from_db = account_kind_from_db,
    unspecified = Unspecified,
    Person => "PERSON",
    ServiceAccount => "SERVICE_ACCOUNT",
    Workload => "WORKLOAD",
    ExternalIdentity => "EXTERNAL_IDENTITY",
    System => "SYSTEM",
    Anonymous => "ANONYMOUS",
}

enum_db_mapping! {
    authn_entity_pb::UserStatus,
    prefix = "USER_STATUS_",
    to_db = user_status_to_db,
    from_db = user_status_from_db,
    unspecified = Unspecified,
    PendingVerification => "PENDING_VERIFICATION",
    Active => "ACTIVE",
    Suspended => "SUSPENDED",
    Locked => "LOCKED",
    Deactivated => "DEACTIVATED",
}

enum_db_mapping! {
    authn_entity_pb::OtpType,
    prefix = "OTP_TYPE_",
    to_db = otp_type_to_db,
    from_db = otp_type_from_db,
    unspecified = Unspecified,
    EmailVerification => "EMAIL_VERIFICATION",
    Login2fa => "LOGIN_2FA",
    PasswordReset => "PASSWORD_RESET",
    SensitiveOperation => "SENSITIVE_OPERATION",
}

enum_db_mapping! {
    authn_entity_pb::OtpStatus,
    prefix = "OTP_STATUS_",
    to_db = otp_status_to_db,
    from_db = otp_status_from_db,
    unspecified = Unspecified,
    Pending => "PENDING",
    Used => "USED",
    Expired => "EXPIRED",
    Invalidated => "INVALIDATED",
}

fn api_key_status_to_db(status: i32) -> &'static str {
    match apikey_entity_pb::ApiKeyStatus::try_from(status).unwrap_or_default() {
        apikey_entity_pb::ApiKeyStatus::Active => "ACTIVE",
        apikey_entity_pb::ApiKeyStatus::Revoked => "REVOKED",
        apikey_entity_pb::ApiKeyStatus::Expired => "EXPIRED",
        apikey_entity_pb::ApiKeyStatus::Unspecified => "UNSPECIFIED",
    }
}

fn session_from_row(row: &sqlx::postgres::PgRow) -> Result<SessionRecord, sqlx::Error> {
    Ok(SessionRecord {
        session_id_hash: row.try_get("session_id_hash")?,
        principal_id: row.try_get("principal_id")?,
        user_id: row.try_get("user_id")?,
        service_identity: row_string(row, "service_identity")?,
        tenant_id: row.try_get("tenant_id")?,
        project_id: row_string(row, "project_id")?,
        scopes: string_list_from_json(&row.try_get::<String, _>("scopes")?),
        roles: string_list_from_json(&row.try_get::<String, _>("roles")?),
        relationship_version: row_string(row, "relationship_version")?,
        created_at_unix: row.try_get::<i64, _>("created_at_unix")?.max(0) as u64,
        updated_at_unix: row.try_get::<i64, _>("updated_at_unix")?.max(0) as u64,
        expires_at_unix: row.try_get::<i64, _>("expires_at_unix")?.max(0) as u64,
        revoked_at_unix: row.try_get::<i64, _>("revoked_at_unix")?.max(0) as u64,
        client_fingerprint: row_string(row, "client_fingerprint")?,
    })
}

fn api_key_from_row(row: &sqlx::postgres::PgRow) -> Result<ApiKeyRecord, sqlx::Error> {
    Ok(ApiKeyRecord {
        key_prefix: row.try_get("key_prefix")?,
        key_hash: row.try_get("key_hash")?,
        principal_id: row.try_get("principal_id")?,
        service_identity: row_string(row, "service_identity")?,
        tenant_id: row_string(row, "tenant_id")?,
        project_id: row_string(row, "project_id")?,
        scopes: string_list_from_json(&row.try_get::<String, _>("scopes")?),
        created_at_unix: row.try_get::<i64, _>("created_at_unix")?.max(0) as u64,
        last_used_at_unix: row.try_get::<i64, _>("last_used_at_unix")?.max(0) as u64,
        expires_at_unix: row.try_get::<i64, _>("expires_at_unix")?.max(0) as u64,
        revoked_at_unix: row.try_get::<i64, _>("revoked_at_unix")?.max(0) as u64,
    })
}

fn user_from_row(row: &sqlx::postgres::PgRow) -> Result<UserRecord, sqlx::Error> {
    Ok(UserRecord {
        user_id: row.try_get("user_id")?,
        username: row.try_get("username")?,
        email: row.try_get("email")?,
        password_hash: row.try_get("password_hash")?,
        account_kind: account_kind_from_db(&row.try_get::<String, _>("account_kind")?),
        status: user_status_from_db(&row.try_get::<String, _>("status")?),
        tenant_id: row.try_get("tenant_id")?,
        full_name: row.try_get("full_name")?,
        totp_secret_hash: row_string(row, "totp_secret_hash")?,
        mfa_enabled: row.try_get("mfa_enabled")?,
        failed_login_count: row.try_get("failed_login_count")?,
        locked_until_unix: row.try_get::<i64, _>("locked_until_unix")?.max(0) as u64,
        email_verified_at_unix: row.try_get::<i64, _>("email_verified_at_unix")?.max(0) as u64,
        last_login_at_unix: row.try_get::<i64, _>("last_login_at_unix")?.max(0) as u64,
        created_by: row.try_get("created_by")?,
        created_at_unix: row.try_get::<i64, _>("created_at_unix")?.max(0) as u64,
        updated_at_unix: row.try_get::<i64, _>("updated_at_unix")?.max(0) as u64,
        deleted_at_unix: row.try_get::<i64, _>("deleted_at_unix")?.max(0) as u64,
        deleted_by: row.try_get("deleted_by")?,
        project_id: row.try_get("project_id")?,
        external_provider_id: row.try_get("external_provider_id")?,
        external_subject: row.try_get("external_subject")?,
        profile_attributes_json: row.try_get("profile_attributes_json")?,
    })
}

fn otp_from_row(row: &sqlx::postgres::PgRow) -> Result<OtpRecord, sqlx::Error> {
    Ok(OtpRecord {
        otp_id: row.try_get("otp_id")?,
        user_id: row.try_get("user_id")?,
        otp_type: otp_type_from_db(&row.try_get::<String, _>("otp_type")?),
        code_hash: row.try_get("code_hash")?,
        delivery_channel: row.try_get("delivery_channel")?,
        delivery_address: row_string(row, "delivery_address")?,
        status: otp_status_from_db(&row.try_get::<String, _>("status")?),
        attempt_count: row.try_get("attempt_count")?,
        superseded_by_id: row_string(row, "superseded_by_id")?,
        expires_at_unix: row.try_get::<i64, _>("expires_at_unix")?.max(0) as u64,
        used_at_unix: row.try_get::<i64, _>("used_at_unix")?.max(0) as u64,
        created_at_unix: row.try_get::<i64, _>("created_at_unix")?.max(0) as u64,
        correlation_id: row.try_get("correlation_id")?,
    })
}

/// Server-side session persistence.
#[async_trait]
pub trait SessionStore: Send + Sync {
    async fn put(&self, record: &SessionRecord) -> Result<(), String>;
    async fn get(&self, session_id_hash: &str) -> Result<Option<SessionRecord>, String>;
    async fn revoke(&self, session_id_hash: &str, now_unix: u64) -> Result<bool, String>;
    async fn touch_last_active(&self, session_id_hash: &str, now_unix: u64) -> Result<(), String> {
        let Some(mut record) = self.get(session_id_hash).await? else {
            return Ok(());
        };
        record.updated_at_unix = now_unix;
        self.put(&record).await
    }
    async fn revoke_all_for_principal(
        &self,
        principal_id: &str,
        now_unix: u64,
    ) -> Result<usize, String>;
    async fn list_for_principal(
        &self,
        principal_id: &str,
        active_only: bool,
        now_unix: u64,
    ) -> Result<Vec<SessionRecord>, String>;
    async fn list_for_principal_page(
        &self,
        principal_id: &str,
        active_only: bool,
        now_unix: u64,
        limit: usize,
        offset: usize,
    ) -> Result<(Vec<SessionRecord>, usize), String> {
        let all = self
            .list_for_principal(principal_id, active_only, now_unix)
            .await?;
        let total = all.len();
        Ok((all.into_iter().skip(offset).take(limit).collect(), total))
    }
}

/// API/service key persistence.
#[async_trait]
pub trait ApiKeyStore: Send + Sync {
    async fn put(&self, record: ApiKeyRecord) -> Result<(), String>;
    async fn get_by_prefix(&self, key_prefix: &str) -> Result<Option<ApiKeyRecord>, String>;
    async fn list_for_principal(
        &self,
        principal_id: &str,
        active_only: bool,
        now_unix: u64,
    ) -> Result<Vec<ApiKeyRecord>, String>;
    async fn list_for_principal_page(
        &self,
        principal_id: &str,
        active_only: bool,
        now_unix: u64,
        limit: usize,
        offset: usize,
    ) -> Result<(Vec<ApiKeyRecord>, usize), String> {
        let all = self
            .list_for_principal(principal_id, active_only, now_unix)
            .await?;
        let total = all.len();
        Ok((all.into_iter().skip(offset).take(limit).collect(), total))
    }
    async fn list_for_principal_status_page(
        &self,
        principal_id: &str,
        status: apikey_entity_pb::ApiKeyStatus,
        now_unix: u64,
        limit: usize,
        offset: usize,
    ) -> Result<(Vec<ApiKeyRecord>, usize), String> {
        let mut all = self
            .list_for_principal(principal_id, false, now_unix)
            .await?;
        match status {
            apikey_entity_pb::ApiKeyStatus::Unspecified => {}
            apikey_entity_pb::ApiKeyStatus::Active => {
                all.retain(|rec| !rec.is_revoked() && !rec.is_expired(now_unix));
            }
            apikey_entity_pb::ApiKeyStatus::Revoked => all.retain(ApiKeyRecord::is_revoked),
            apikey_entity_pb::ApiKeyStatus::Expired => {
                all.retain(|rec| !rec.is_revoked() && rec.is_expired(now_unix));
            }
        }
        let total = all.len();
        Ok((all.into_iter().skip(offset).take(limit).collect(), total))
    }
    async fn revoke(&self, key_prefix: &str, now_unix: u64) -> Result<bool, String>;
    /// Best-effort `last_used_at` stamp on a successful validation. Default
    /// no-op so non-persistent/unavailable stores need not implement it; the
    /// Postgres store overrides it with an UPDATE.
    async fn touch_last_used(&self, _key_prefix: &str, _now_unix: u64) -> Result<(), String> {
        Ok(())
    }
    /// Atomically replace the key identified by `old_key_prefix` with
    /// `new_record` (#209). The default does put-then-revoke, which is NOT
    /// atomic — a revoke failure after a successful put leaves both keys active
    /// (split-brain). Persistent stores override this with a single transaction
    /// so the insert + revoke commit or roll back together.
    async fn rotate(
        &self,
        old_key_prefix: &str,
        new_record: ApiKeyRecord,
        now_unix: u64,
    ) -> Result<(), String> {
        self.put(new_record).await?;
        self.revoke(old_key_prefix, now_unix).await?;
        Ok(())
    }
}

#[async_trait]
pub trait UserStore: Send + Sync {
    async fn put_user(&self, record: UserRecord) -> Result<(), String>;
    async fn get_user_by_id(&self, user_id: &str) -> Result<Option<UserRecord>, String>;
    async fn get_user_by_username(&self, username: &str) -> Result<Option<UserRecord>, String>;
    async fn get_user_by_email(&self, email: &str) -> Result<Option<UserRecord>, String>;
    async fn list_users(
        &self,
        tenant_id: &str,
        account_kind: i32,
        status: i32,
    ) -> Result<Vec<UserRecord>, String>;
    async fn list_users_page(
        &self,
        tenant_id: &str,
        account_kind: i32,
        status: i32,
        limit: usize,
        offset: usize,
    ) -> Result<(Vec<UserRecord>, usize), String> {
        let all = self.list_users(tenant_id, account_kind, status).await?;
        let total = all.len();
        Ok((all.into_iter().skip(offset).take(limit).collect(), total))
    }
    async fn delete_user(
        &self,
        user_id: &str,
        deleted_by: &str,
        now_unix: u64,
    ) -> Result<bool, String>;
    async fn put_otp(&self, record: OtpRecord) -> Result<(), String>;
    async fn get_otp(&self, otp_id: &str) -> Result<Option<OtpRecord>, String>;
    async fn update_otp(&self, record: OtpRecord) -> Result<(), String>;
}

/// Store placeholder used when native auth is enabled without Postgres wiring.
/// It fails closed; tests/local helpers must opt in to the in-memory stores.
#[derive(Default)]
pub struct UnavailableSessionStore;

#[async_trait]
impl SessionStore for UnavailableSessionStore {
    async fn put(&self, _record: &SessionRecord) -> Result<(), String> {
        Err("Postgres session store is not configured".to_string())
    }

    async fn get(&self, _session_id_hash: &str) -> Result<Option<SessionRecord>, String> {
        Err("Postgres session store is not configured".to_string())
    }

    async fn revoke(&self, _session_id_hash: &str, _now_unix: u64) -> Result<bool, String> {
        Err("Postgres session store is not configured".to_string())
    }

    async fn revoke_all_for_principal(
        &self,
        _principal_id: &str,
        _now_unix: u64,
    ) -> Result<usize, String> {
        Err("Postgres session store is not configured".to_string())
    }

    async fn list_for_principal(
        &self,
        _principal_id: &str,
        _active_only: bool,
        _now_unix: u64,
    ) -> Result<Vec<SessionRecord>, String> {
        Err("Postgres session store is not configured".to_string())
    }
}

/// API-key store placeholder used by production constructors without Postgres.
#[derive(Default)]
pub struct UnavailableApiKeyStore;

#[async_trait]
impl ApiKeyStore for UnavailableApiKeyStore {
    async fn put(&self, _record: ApiKeyRecord) -> Result<(), String> {
        Err("Postgres API-key store is not configured".to_string())
    }

    async fn get_by_prefix(&self, _key_prefix: &str) -> Result<Option<ApiKeyRecord>, String> {
        Err("Postgres API-key store is not configured".to_string())
    }

    async fn list_for_principal(
        &self,
        _principal_id: &str,
        _active_only: bool,
        _now_unix: u64,
    ) -> Result<Vec<ApiKeyRecord>, String> {
        Err("Postgres API-key store is not configured".to_string())
    }

    async fn revoke(&self, _key_prefix: &str, _now_unix: u64) -> Result<bool, String> {
        Err("Postgres API-key store is not configured".to_string())
    }
}

#[derive(Default)]
pub struct UnavailableUserStore;

#[async_trait]
impl UserStore for UnavailableUserStore {
    async fn put_user(&self, _record: UserRecord) -> Result<(), String> {
        Err("Postgres user store is not configured".to_string())
    }

    async fn get_user_by_id(&self, _user_id: &str) -> Result<Option<UserRecord>, String> {
        Err("Postgres user store is not configured".to_string())
    }

    async fn get_user_by_username(&self, _username: &str) -> Result<Option<UserRecord>, String> {
        Err("Postgres user store is not configured".to_string())
    }

    async fn get_user_by_email(&self, _email: &str) -> Result<Option<UserRecord>, String> {
        Err("Postgres user store is not configured".to_string())
    }

    async fn list_users(
        &self,
        _tenant_id: &str,
        _account_kind: i32,
        _status: i32,
    ) -> Result<Vec<UserRecord>, String> {
        Err("Postgres user store is not configured".to_string())
    }

    async fn delete_user(
        &self,
        _user_id: &str,
        _deleted_by: &str,
        _now_unix: u64,
    ) -> Result<bool, String> {
        Err("Postgres user store is not configured".to_string())
    }

    async fn put_otp(&self, _record: OtpRecord) -> Result<(), String> {
        Err("Postgres user store is not configured".to_string())
    }

    async fn get_otp(&self, _otp_id: &str) -> Result<Option<OtpRecord>, String> {
        Err("Postgres user store is not configured".to_string())
    }

    async fn update_otp(&self, _record: OtpRecord) -> Result<(), String> {
        Err("Postgres user store is not configured".to_string())
    }
}

// ── Native service catalog DDL ──────────────────────────────────────────────

/// Back-compat wrapper for older call sites. Native authn DDL is generated from
/// `proto/udb/core/authn/entity/**` through the normal UDB proto migration path.
pub fn auth_catalog_ddl(_schema: &str) -> Vec<String> {
    crate::runtime::native_catalog::native_service_catalog_ddl()
        .into_iter()
        .filter(|sql| sql.contains("udb_authn"))
        .collect()
}

/// Postgres-backed session store for native auth serving.
pub struct PostgresSessionStore {
    pool: PgPool,
    model: NativeModel,
}

impl PostgresSessionStore {
    pub fn new(pool: PgPool, schema: impl Into<String>) -> Self {
        let _ = schema.into();
        Self {
            pool,
            model: native_model(
                "udb.core.authn.entity.v1.Session",
                &[
                    "session_id",
                    "user_id",
                    "session_type",
                    "session_token_lookup",
                    "session_token_hash",
                    "device_type",
                    "device_name",
                    "is_active",
                    "expires_at",
                    "last_active_at",
                    "created_at",
                    "tenant_id",
                    "project_id",
                    "principal_id",
                    "auth_method",
                    "scopes_json",
                    "metadata_json",
                    "revoke_reason",
                ],
            ),
        }
    }

    fn relation(&self) -> String {
        self.model.relation.clone()
    }
}

#[async_trait]
impl SessionStore for PostgresSessionStore {
    async fn put(&self, record: &SessionRecord) -> Result<(), String> {
        let rel = self.relation();
        let m = &self.model;
        let session_id = m.q("session_id");
        let principal_id = m.q("principal_id");
        let user_id = m.q("user_id");
        let tenant_id = m.q("tenant_id");
        let project_id = m.q("project_id");
        let scopes_json = m.q("scopes_json");
        let metadata_json_col = m.q("metadata_json");
        let last_active_at = m.q("last_active_at");
        let expires_at = m.q("expires_at");
        let is_active = m.q("is_active");
        let revoke_reason = m.q("revoke_reason");
        let session_type = m.q("session_type");
        let session_token_lookup = m.q("session_token_lookup");
        let session_token_hash = m.q("session_token_hash");
        let device_type = m.q("device_type");
        let device_name = m.q("device_name");
        let auth_method = m.q("auth_method");
        let metadata_json = serde_json::json!({
            "roles": record.roles,
            "relationship_version": record.relationship_version,
            "client_fingerprint": record.client_fingerprint,
            "service_identity": record.service_identity,
        })
        .to_string();
        sqlx::query(&format!(
            "WITH updated AS ( \
               UPDATE {rel} SET {principal_id} = $2, {user_id} = $3::UUID, {tenant_id} = $4, {project_id} = $5, {scopes_json} = $6::JSONB, \
                 {metadata_json_col} = $7::JSONB, {last_active_at} = to_timestamp($9::DOUBLE PRECISION), \
                 {expires_at} = to_timestamp($10::DOUBLE PRECISION), {is_active} = ($11::BIGINT = 0), \
                 {revoke_reason} = CASE WHEN $11::BIGINT > 0 THEN 'revoked' ELSE '' END \
               WHERE {session_token_lookup} = $1 RETURNING {session_id} \
             ) \
             INSERT INTO {rel} \
               ({user_id}, {session_type}, {session_token_lookup}, {session_token_hash}, {device_type}, {device_name}, {is_active}, {expires_at}, {last_active_at}, {tenant_id}, {project_id}, {principal_id}, {auth_method}, {scopes_json}, {metadata_json_col}) \
             SELECT $3::UUID, 'SERVER_SIDE', $1, $1, 'API', $8, ($11::BIGINT = 0), to_timestamp($10::DOUBLE PRECISION), \
                    to_timestamp($9::DOUBLE PRECISION), $4, $5, $2, 'session', $6::JSONB, $7::JSONB \
             WHERE NOT EXISTS (SELECT 1 FROM updated)"
        ))
        .bind(&record.session_id_hash)
        .bind(&record.principal_id)
        .bind(&record.user_id)
        .bind(&record.tenant_id)
        .bind(&record.project_id)
        .bind(string_list_to_json(&record.scopes))
        .bind(metadata_json)
        .bind(&record.client_fingerprint)
        .bind(record.updated_at_unix as i64)
        .bind(record.expires_at_unix as i64)
        .bind(record.revoked_at_unix as i64)
        .execute(&self.pool)
        .await
        .map_err(|err| format!("put session failed: {err}"))?;
        Ok(())
    }

    async fn get(&self, session_id_hash: &str) -> Result<Option<SessionRecord>, String> {
        let rel = self.relation();
        let m = &self.model;
        let session_token_lookup = m.q("session_token_lookup");
        let principal_id_select = m.select("principal_id");
        let user_id_select = m.text("user_id");
        let service_identity =
            m.json_get_as("metadata_json", "service_identity", "service_identity");
        let tenant_id_select = m.select("tenant_id");
        let project_id = m.select("project_id");
        let scopes = m.json_text_as("scopes_json", "scopes");
        let roles = m.json_coalesce_as("metadata_json", "roles", "[]", "roles");
        let relationship_version = m.json_get_as(
            "metadata_json",
            "relationship_version",
            "relationship_version",
        );
        let client_fingerprint =
            m.json_get_as("metadata_json", "client_fingerprint", "client_fingerprint");
        let created_at_unix = m.timestamp_unix_as("created_at", "created_at_unix");
        let updated_at_unix = m.timestamp_unix_as("last_active_at", "updated_at_unix");
        let expires_at_unix = m.timestamp_unix_as("expires_at", "expires_at_unix");
        let is_active = m.q("is_active");
        let last_active_at = m.q("last_active_at");
        let row = sqlx::query(&format!(
            "SELECT {session_token_lookup} AS session_id_hash, {principal_id_select}, {user_id_select}, {service_identity}, {tenant_id_select}, {project_id}, {scopes}, \
                    {roles}, {relationship_version}, {client_fingerprint}, \
                    {created_at_unix}, {updated_at_unix}, {expires_at_unix}, \
                    CASE WHEN {is_active} THEN 0 ELSE COALESCE(EXTRACT(EPOCH FROM {last_active_at})::BIGINT, 1) END AS revoked_at_unix \
             FROM {rel} WHERE {session_token_lookup} = $1"
        ))
        .bind(session_id_hash)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| format!("get session failed: {err}"))?;
        row.as_ref()
            .map(session_from_row)
            .transpose()
            .map_err(|err| format!("decode session failed: {err}"))
    }

    async fn revoke(&self, session_id_hash: &str, now_unix: u64) -> Result<bool, String> {
        let rel = self.relation();
        let session_token_lookup = self.model.q("session_token_lookup");
        let is_active = self.model.q("is_active");
        let last_active_at = self.model.q("last_active_at");
        let revoke_reason = self.model.q("revoke_reason");
        let result = sqlx::query(&format!(
            "UPDATE {rel} SET {is_active} = FALSE, {last_active_at} = to_timestamp($2::DOUBLE PRECISION), {revoke_reason} = 'revoked' \
             WHERE {session_token_lookup} = $1 AND {is_active} = TRUE"
        ))
        .bind(session_id_hash)
        .bind(now_unix as i64)
        .execute(&self.pool)
        .await
        .map_err(|err| format!("revoke session failed: {err}"))?;
        Ok(result.rows_affected() > 0)
    }

    async fn touch_last_active(&self, session_id_hash: &str, now_unix: u64) -> Result<(), String> {
        let rel = self.relation();
        let session_token_lookup = self.model.q("session_token_lookup");
        let is_active = self.model.q("is_active");
        let last_active_at = self.model.q("last_active_at");
        sqlx::query(&format!(
            "UPDATE {rel} SET {last_active_at} = to_timestamp($2::DOUBLE PRECISION) \
             WHERE {session_token_lookup} = $1 AND {is_active} = TRUE"
        ))
        .bind(session_id_hash)
        .bind(now_unix as i64)
        .execute(&self.pool)
        .await
        .map_err(|err| format!("touch session last_active_at failed: {err}"))?;
        Ok(())
    }

    async fn revoke_all_for_principal(
        &self,
        principal_id: &str,
        now_unix: u64,
    ) -> Result<usize, String> {
        let rel = self.relation();
        let principal_id_col = self.model.q("principal_id");
        let is_active = self.model.q("is_active");
        let last_active_at = self.model.q("last_active_at");
        let revoke_reason = self.model.q("revoke_reason");
        let result = sqlx::query(&format!(
            "UPDATE {rel} SET {is_active} = FALSE, {last_active_at} = to_timestamp($2::DOUBLE PRECISION), {revoke_reason} = 'principal_revoke' \
             WHERE {principal_id_col} = $1 AND {is_active} = TRUE"
        ))
        .bind(principal_id)
        .bind(now_unix as i64)
        .execute(&self.pool)
        .await
        .map_err(|err| format!("revoke principal sessions failed: {err}"))?;
        Ok(result.rows_affected() as usize)
    }

    async fn list_for_principal(
        &self,
        principal_id: &str,
        active_only: bool,
        now_unix: u64,
    ) -> Result<Vec<SessionRecord>, String> {
        let rel = self.relation();
        let m = &self.model;
        let session_token_lookup = m.q("session_token_lookup");
        let principal_id_col = m.q("principal_id");
        let user_id_col = format!("{}::TEXT", m.q("user_id"));
        let principal_id_select = m.select("principal_id");
        let user_id_select = m.text("user_id");
        let service_identity =
            m.json_get_as("metadata_json", "service_identity", "service_identity");
        let tenant_id_select = m.select("tenant_id");
        let project_id = m.select("project_id");
        let scopes = m.json_text_as("scopes_json", "scopes");
        let roles = m.json_coalesce_as("metadata_json", "roles", "[]", "roles");
        let relationship_version = m.json_get_as(
            "metadata_json",
            "relationship_version",
            "relationship_version",
        );
        let client_fingerprint =
            m.json_get_as("metadata_json", "client_fingerprint", "client_fingerprint");
        let created_at_unix = m.timestamp_unix_as("created_at", "created_at_unix");
        let updated_at_unix = m.timestamp_unix_as("last_active_at", "updated_at_unix");
        let expires_at_unix = m.timestamp_unix_as("expires_at", "expires_at_unix");
        let is_active = m.q("is_active");
        let last_active_at = m.q("last_active_at");
        let expires_at = m.q("expires_at");
        let active_clause = if active_only {
            format!("AND {is_active} = TRUE AND {expires_at} > to_timestamp($2::DOUBLE PRECISION)")
        } else {
            String::new()
        };
        let sql = format!(
            "SELECT {session_token_lookup} AS session_id_hash, {principal_id_select}, {user_id_select}, {service_identity}, {tenant_id_select}, {project_id}, {scopes}, \
                    {roles}, {relationship_version}, {client_fingerprint}, \
                    {created_at_unix}, {updated_at_unix}, {expires_at_unix}, \
                    CASE WHEN {is_active} THEN 0 ELSE COALESCE(EXTRACT(EPOCH FROM {last_active_at})::BIGINT, 1) END AS revoked_at_unix \
             FROM {rel} WHERE ({principal_id_col} = $1 OR {user_id_col} = $1) {active_clause} ORDER BY {last_active_at} DESC"
        );
        let mut query = sqlx::query(&sql).bind(principal_id);
        if active_only {
            query = query.bind(now_unix as i64);
        }
        let rows = query
            .fetch_all(&self.pool)
            .await
            .map_err(|err| format!("list sessions failed: {err}"))?;
        rows.iter()
            .map(session_from_row)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| format!("decode sessions failed: {err}"))
    }

    async fn list_for_principal_page(
        &self,
        principal_id: &str,
        active_only: bool,
        now_unix: u64,
        limit: usize,
        offset: usize,
    ) -> Result<(Vec<SessionRecord>, usize), String> {
        let rel = self.relation();
        let m = &self.model;
        let session_token_lookup = m.q("session_token_lookup");
        let principal_id_col = m.q("principal_id");
        let user_id_col = format!("{}::TEXT", m.q("user_id"));
        let principal_id_select = m.select("principal_id");
        let user_id_select = m.text("user_id");
        let service_identity =
            m.json_get_as("metadata_json", "service_identity", "service_identity");
        let tenant_id_select = m.select("tenant_id");
        let project_id = m.select("project_id");
        let scopes = m.json_text_as("scopes_json", "scopes");
        let roles = m.json_coalesce_as("metadata_json", "roles", "[]", "roles");
        let relationship_version = m.json_get_as(
            "metadata_json",
            "relationship_version",
            "relationship_version",
        );
        let client_fingerprint =
            m.json_get_as("metadata_json", "client_fingerprint", "client_fingerprint");
        let created_at_unix = m.timestamp_unix_as("created_at", "created_at_unix");
        let updated_at_unix = m.timestamp_unix_as("last_active_at", "updated_at_unix");
        let expires_at_unix = m.timestamp_unix_as("expires_at", "expires_at_unix");
        let is_active = m.q("is_active");
        let last_active_at = m.q("last_active_at");
        let expires_at = m.q("expires_at");
        let active_clause = if active_only {
            format!("AND {is_active} = TRUE AND {expires_at} > to_timestamp($2::DOUBLE PRECISION)")
        } else {
            String::new()
        };
        let count_sql = format!(
            "SELECT COUNT(*)::BIGINT AS total_count FROM {rel} WHERE ({principal_id_col} = $1 OR {user_id_col} = $1) {active_clause}"
        );
        let mut count_query = sqlx::query(&count_sql).bind(principal_id);
        if active_only {
            count_query = count_query.bind(now_unix as i64);
        }
        let total = count_query
            .fetch_one(&self.pool)
            .await
            .map_err(|err| format!("count sessions failed: {err}"))?
            .try_get::<i64, _>("total_count")
            .map_err(|err| format!("decode session count failed: {err}"))?
            .max(0) as usize;
        let (limit_param, offset_param) = if active_only {
            ("$3", "$4")
        } else {
            ("$2", "$3")
        };
        let sql = format!(
            "SELECT {session_token_lookup} AS session_id_hash, {principal_id_select}, {user_id_select}, {service_identity}, {tenant_id_select}, {project_id}, {scopes}, \
                    {roles}, {relationship_version}, {client_fingerprint}, \
                    {created_at_unix}, {updated_at_unix}, {expires_at_unix}, \
                    CASE WHEN {is_active} THEN 0 ELSE COALESCE(EXTRACT(EPOCH FROM {last_active_at})::BIGINT, 1) END AS revoked_at_unix \
             FROM {rel} WHERE ({principal_id_col} = $1 OR {user_id_col} = $1) {active_clause} ORDER BY {last_active_at} DESC LIMIT {limit_param} OFFSET {offset_param}"
        );
        let mut query = sqlx::query(&sql).bind(principal_id);
        if active_only {
            query = query.bind(now_unix as i64);
        }
        let rows = query
            .bind(limit as i64)
            .bind(offset as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|err| format!("list sessions failed: {err}"))?;
        let items = rows
            .iter()
            .map(session_from_row)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| format!("decode sessions failed: {err}"))?;
        Ok((items, total))
    }
}

#[cfg(feature = "redis")]
pub struct RedisSessionStore {
    client: redis::Client,
    key_prefix: String,
    ttl_secs: u64,
}

#[cfg(feature = "redis")]
impl RedisSessionStore {
    pub fn new(client: redis::Client, key_prefix: impl Into<String>, ttl_secs: u64) -> Self {
        Self {
            client,
            key_prefix: key_prefix.into(),
            ttl_secs,
        }
    }

    pub fn from_url(
        url: &str,
        key_prefix: impl Into<String>,
        ttl_secs: u64,
    ) -> Result<Self, String> {
        redis::Client::open(url)
            .map(|client| Self::new(client, key_prefix, ttl_secs))
            .map_err(|err| format!("invalid Redis session URL: {err}"))
    }

    fn session_key(&self, session_id_hash: &str) -> String {
        format!("{}:session:{}", self.key_prefix, session_id_hash)
    }

    fn principal_key(&self, principal_id: &str) -> String {
        format!("{}:principal:{}", self.key_prefix, principal_id)
    }

    async fn connection(&self) -> Result<redis::aio::MultiplexedConnection, String> {
        self.client
            .get_multiplexed_async_connection()
            .await
            .map_err(|err| format!("redis session connection failed: {err}"))
    }
}

#[cfg(feature = "redis")]
#[async_trait]
impl SessionStore for RedisSessionStore {
    async fn put(&self, record: &SessionRecord) -> Result<(), String> {
        let mut conn = self.connection().await?;
        let key = self.session_key(&record.session_id_hash);
        let index_key = self.principal_key(&record.principal_id);
        let ttl = if record.expires_at_unix > 0 {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);
            record.expires_at_unix.saturating_sub(now).max(1)
        } else {
            self.ttl_secs.max(1)
        };
        let json = serde_json::to_string(&record)
            .map_err(|err| format!("encode redis session failed: {err}"))?;
        let _: () = conn
            .set_ex(&key, json, ttl)
            .await
            .map_err(|err| format!("redis session SETEX failed: {err}"))?;
        let _: () = conn
            .sadd(&index_key, &record.session_id_hash)
            .await
            .map_err(|err| format!("redis session SADD failed: {err}"))?;
        let _: () = conn
            .expire(&index_key, ttl as i64)
            .await
            .map_err(|err| format!("redis session EXPIRE failed: {err}"))?;
        Ok(())
    }

    async fn get(&self, session_id_hash: &str) -> Result<Option<SessionRecord>, String> {
        let mut conn = self.connection().await?;
        let key = self.session_key(session_id_hash);
        let raw: Option<String> = conn
            .get(&key)
            .await
            .map_err(|err| format!("redis session GET failed: {err}"))?;
        raw.map(|json| {
            serde_json::from_str(&json).map_err(|err| format!("decode redis session failed: {err}"))
        })
        .transpose()
    }

    async fn revoke(&self, session_id_hash: &str, now_unix: u64) -> Result<bool, String> {
        let Some(mut rec) = self.get(session_id_hash).await? else {
            return Ok(false);
        };
        rec.revoked_at_unix = now_unix.max(1);
        self.put(&rec).await?;
        Ok(true)
    }

    async fn revoke_all_for_principal(
        &self,
        principal_id: &str,
        now_unix: u64,
    ) -> Result<usize, String> {
        let mut conn = self.connection().await?;
        let index_key = self.principal_key(principal_id);
        let ids: Vec<String> = conn
            .smembers(&index_key)
            .await
            .map_err(|err| format!("redis session SMEMBERS failed: {err}"))?;
        if ids.is_empty() {
            return Ok(0);
        }
        let keys = ids
            .iter()
            .map(|id| self.session_key(id))
            .collect::<Vec<_>>();
        let raws: Vec<Option<String>> = conn
            .get(&keys)
            .await
            .map_err(|err| format!("redis session MGET failed: {err}"))?;
        let mut pipe = redis::pipe();
        let mut revoked = 0usize;
        for raw in raws.into_iter().flatten() {
            let mut rec: SessionRecord = serde_json::from_str(&raw)
                .map_err(|err| format!("decode redis session failed: {err}"))?;
            if rec.revoked_at_unix > 0 {
                continue;
            }
            rec.revoked_at_unix = now_unix.max(1);
            let key = self.session_key(&rec.session_id_hash);
            let ttl = self.ttl_secs.max(1);
            let json = serde_json::to_string(&rec)
                .map_err(|err| format!("encode redis session failed: {err}"))?;
            pipe.cmd("SETEX").arg(key).arg(ttl).arg(json);
            revoked += 1;
        }
        if revoked > 0 {
            let _: () = pipe
                .query_async(&mut conn)
                .await
                .map_err(|err| format!("redis session revoke pipeline failed: {err}"))?;
        }
        Ok(revoked)
    }

    async fn list_for_principal(
        &self,
        principal_id: &str,
        active_only: bool,
        now_unix: u64,
    ) -> Result<Vec<SessionRecord>, String> {
        let mut conn = self.connection().await?;
        let index_key = self.principal_key(principal_id);
        let ids: Vec<String> = conn
            .smembers(&index_key)
            .await
            .map_err(|err| format!("redis session SMEMBERS failed: {err}"))?;
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let keys = ids
            .iter()
            .map(|id| self.session_key(id))
            .collect::<Vec<_>>();
        let raws: Vec<Option<String>> = conn
            .get(&keys)
            .await
            .map_err(|err| format!("redis session MGET failed: {err}"))?;
        let mut out = Vec::new();
        for raw in raws.into_iter().flatten() {
            let rec: SessionRecord = serde_json::from_str(&raw)
                .map_err(|err| format!("decode redis session failed: {err}"))?;
            if !active_only || rec.is_active(now_unix) {
                out.push(rec);
            }
        }
        Ok(out)
    }
}

/// Postgres-backed API-key store for native auth serving.
pub struct PostgresApiKeyStore {
    pool: PgPool,
    model: NativeModel,
}

/// Bind the 11 positional params for the api-key INSERT (`put_sql`), shared by
/// `put` and the atomic `rotate` so the bind order lives in one place (#209).
fn bind_api_key_insert<'q>(
    query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    record: &'q ApiKeyRecord,
    status: i32,
    metadata_json: String,
) -> sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
    query
        .bind(&record.key_prefix)
        .bind(&record.key_hash)
        .bind(&record.principal_id)
        .bind(string_list_to_json(&record.scopes))
        .bind(api_key_status_to_db(status))
        .bind(record.last_used_at_unix as i64)
        .bind(record.expires_at_unix as i64)
        .bind(&record.tenant_id)
        .bind(&record.project_id)
        .bind(metadata_json)
        .bind(record.revoked_at_unix as i64)
}

impl PostgresApiKeyStore {
    /// The `INSERT … ON CONFLICT` used by both `put` and the atomic `rotate`
    /// (#209). Binds are positional `$1..$11` (see `bind_put_record`).
    fn put_sql(&self) -> String {
        let rel = self.relation();
        let m = &self.model;
        let key_prefix = m.q("key_prefix");
        let key_hash = m.q("key_hash");
        let name = m.q("name");
        let description = m.q("description");
        let owner_type = m.q("owner_type");
        let owner_id = m.q("owner_id");
        let scopes_json = m.q("scopes_json");
        let status_col = m.q("status");
        let last_used_at = m.q("last_used_at");
        let expires_at = m.q("expires_at");
        let created_by = m.q("created_by");
        let tenant_id = m.q("tenant_id");
        let project_id = m.q("project_id");
        let metadata_json_col = m.q("metadata_json");
        let deleted_at = m.q("deleted_at");
        let deleted_by = m.q("deleted_by");
        format!(
            "INSERT INTO {rel} \
             ({key_prefix}, {key_hash}, {name}, {description}, {owner_type}, {owner_id}, {scopes_json}, {status_col}, {last_used_at}, {expires_at}, {created_by}, {tenant_id}, {project_id}, {metadata_json_col}, {deleted_at}, {deleted_by}) \
             VALUES ($1, $2, $1, '', 'SERVICE_ACCOUNT', $3, $4::JSONB, $5, \
                     CASE WHEN $6::BIGINT > 0 THEN to_timestamp($6::DOUBLE PRECISION) ELSE NULL END, \
                     CASE WHEN $7::BIGINT > 0 THEN to_timestamp($7::DOUBLE PRECISION) ELSE NULL END, \
                     $3, $8, $9, $10::JSONB, \
                     CASE WHEN $11::BIGINT > 0 THEN to_timestamp($11::DOUBLE PRECISION) ELSE NULL END, \
                     CASE WHEN $11::BIGINT > 0 THEN $3 ELSE '' END) \
             ON CONFLICT ({key_hash}) DO UPDATE SET \
               {key_prefix} = EXCLUDED.{key_prefix}, {owner_id} = EXCLUDED.{owner_id}, {scopes_json} = EXCLUDED.{scopes_json}, {status_col} = EXCLUDED.{status_col}, \
               {last_used_at} = EXCLUDED.{last_used_at}, {expires_at} = EXCLUDED.{expires_at}, {tenant_id} = EXCLUDED.{tenant_id}, {project_id} = EXCLUDED.{project_id}, \
               {metadata_json_col} = EXCLUDED.{metadata_json_col}, {deleted_at} = EXCLUDED.{deleted_at}, {deleted_by} = EXCLUDED.{deleted_by}"
        )
    }

    /// The revoke `UPDATE`, parameterized so `rotate` can renumber the binds.
    /// `prefix_param`/`now_param` are the 1-based positional placeholders the
    /// caller will bind (`put`/`revoke` use `$1`/`$2`; `rotate` uses `$12`/`$13`).
    fn revoke_sql(&self, prefix_param: u8, now_param: u8) -> String {
        let rel = self.relation();
        let m = &self.model;
        let status_col = m.q("status");
        let deleted_at = m.q("deleted_at");
        let deleted_by = m.q("deleted_by");
        let revoked_by = m.q("revoked_by");
        let revoke_reason = m.q("revoke_reason");
        let owner_id = m.q("owner_id");
        let key_prefix_col = m.q("key_prefix");
        format!(
            "UPDATE {rel} SET {status_col} = 'REVOKED', {deleted_at} = to_timestamp(${now_param}::DOUBLE PRECISION), {deleted_by} = {owner_id}, {revoked_by} = {owner_id}, {revoke_reason} = 'revoked' \
             WHERE {key_prefix_col} = ${prefix_param} AND {deleted_at} IS NULL"
        )
    }

    pub fn new(pool: PgPool, schema: impl Into<String>) -> Self {
        let _ = schema.into();
        Self {
            pool,
            model: native_model(
                "udb.core.apikey.entity.v1.ApiKey",
                &[
                    "key_prefix",
                    "key_hash",
                    "name",
                    "description",
                    "owner_type",
                    "owner_id",
                    "scopes_json",
                    "status",
                    "last_used_at",
                    "expires_at",
                    "created_at",
                    "created_by",
                    "tenant_id",
                    "project_id",
                    "metadata_json",
                    "deleted_at",
                    "deleted_by",
                    "revoked_by",
                    "revoke_reason",
                ],
            ),
        }
    }

    fn relation(&self) -> String {
        self.model.relation.clone()
    }
}

#[async_trait]
impl ApiKeyStore for PostgresApiKeyStore {
    async fn put(&self, record: ApiKeyRecord) -> Result<(), String> {
        let status = if record.revoked_at_unix > 0 {
            apikey_entity_pb::ApiKeyStatus::Revoked as i32
        } else {
            apikey_entity_pb::ApiKeyStatus::Active as i32
        };
        let metadata_json = serde_json::json!({
            "service_identity": record.service_identity,
        })
        .to_string();
        bind_api_key_insert(sqlx::query(&self.put_sql()), &record, status, metadata_json)
            .execute(&self.pool)
            .await
            .map_err(|err| format!("put api key failed: {err}"))?;
        Ok(())
    }

    async fn rotate(
        &self,
        old_key_prefix: &str,
        new_record: ApiKeyRecord,
        now_unix: u64,
    ) -> Result<(), String> {
        // #209: insert-new + revoke-old in ONE transaction so a revoke failure
        // after a successful insert can't leave both keys active (split-brain).
        let status = if new_record.revoked_at_unix > 0 {
            apikey_entity_pb::ApiKeyStatus::Revoked as i32
        } else {
            apikey_entity_pb::ApiKeyStatus::Active as i32
        };
        let metadata_json = serde_json::json!({
            "service_identity": new_record.service_identity,
        })
        .to_string();
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|err| format!("rotate api key begin failed: {err}"))?;
        bind_api_key_insert(
            sqlx::query(&self.put_sql()),
            &new_record,
            status,
            metadata_json,
        )
        .execute(&mut *tx)
        .await
        .map_err(|err| format!("rotate api key insert failed: {err}"))?;
        sqlx::query(&self.revoke_sql(1, 2))
            .bind(old_key_prefix)
            .bind(now_unix as i64)
            .execute(&mut *tx)
            .await
            .map_err(|err| format!("rotate api key revoke failed: {err}"))?;
        tx.commit()
            .await
            .map_err(|err| format!("rotate api key commit failed: {err}"))?;
        Ok(())
    }

    async fn get_by_prefix(&self, key_prefix: &str) -> Result<Option<ApiKeyRecord>, String> {
        let rel = self.relation();
        let m = &self.model;
        let key_prefix_col = m.q("key_prefix");
        let key_hash = m.select("key_hash");
        let owner_id = m.select_as("owner_id", "principal_id");
        let service_identity =
            m.json_get_as("metadata_json", "service_identity", "service_identity");
        let tenant_id = m.select("tenant_id");
        let project_id = m.select("project_id");
        let scopes = m.json_text_as("scopes_json", "scopes");
        let created_at_unix = m.timestamp_unix_as("created_at", "created_at_unix");
        let last_used_at_unix = m.timestamp_unix_as("last_used_at", "last_used_at_unix");
        let expires_at_unix = m.timestamp_unix_as("expires_at", "expires_at_unix");
        let revoked_at_unix = m.timestamp_unix_as("deleted_at", "revoked_at_unix");
        let created_at = m.q("created_at");
        let deleted_at = m.q("deleted_at");
        let row = sqlx::query(&format!(
            "SELECT {key_prefix_col} AS key_prefix, {key_hash}, {owner_id}, {service_identity}, {tenant_id}, {project_id}, {scopes}, \
                    {created_at_unix}, {last_used_at_unix}, {expires_at_unix}, {revoked_at_unix} \
             FROM {rel} WHERE {key_prefix_col} = $1 AND {deleted_at} IS NULL ORDER BY {created_at} DESC LIMIT 1"
        ))
        .bind(key_prefix)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| format!("get api key failed: {err}"))?;
        row.as_ref()
            .map(api_key_from_row)
            .transpose()
            .map_err(|err| format!("decode api key failed: {err}"))
    }

    async fn list_for_principal(
        &self,
        principal_id: &str,
        active_only: bool,
        now_unix: u64,
    ) -> Result<Vec<ApiKeyRecord>, String> {
        let rel = self.relation();
        let m = &self.model;
        let key_prefix_col = m.select("key_prefix");
        let key_hash = m.select("key_hash");
        let owner_id = m.select_as("owner_id", "principal_id");
        let owner_id_col = m.q("owner_id");
        let service_identity =
            m.json_get_as("metadata_json", "service_identity", "service_identity");
        let service_identity_expr = format!("{}->>'service_identity'", m.q("metadata_json"));
        let tenant_id = m.select("tenant_id");
        let project_id = m.select("project_id");
        let scopes = m.json_text_as("scopes_json", "scopes");
        let created_at_unix = m.timestamp_unix_as("created_at", "created_at_unix");
        let last_used_at_unix = m.timestamp_unix_as("last_used_at", "last_used_at_unix");
        let expires_at_unix = m.timestamp_unix_as("expires_at", "expires_at_unix");
        let revoked_at_unix = m.timestamp_unix_as("deleted_at", "revoked_at_unix");
        let deleted_at = m.q("deleted_at");
        let status_col = m.q("status");
        let expires_at = m.q("expires_at");
        let created_at = m.q("created_at");
        let active_clause = if active_only {
            format!(
                "AND {deleted_at} IS NULL AND {status_col} = 'ACTIVE' AND ({expires_at} IS NULL OR {expires_at} > to_timestamp($2::DOUBLE PRECISION))"
            )
        } else {
            String::new()
        };
        let sql = format!(
            "SELECT {key_prefix_col}, {key_hash}, {owner_id}, {service_identity}, {tenant_id}, {project_id}, {scopes}, \
                    {created_at_unix}, {last_used_at_unix}, {expires_at_unix}, {revoked_at_unix} \
             FROM {rel} WHERE ({owner_id_col} = $1 OR {service_identity_expr} = $1) {active_clause} ORDER BY {created_at} DESC"
        );
        let mut query = sqlx::query(&sql).bind(principal_id);
        if active_only {
            query = query.bind(now_unix as i64);
        }
        let rows = query
            .fetch_all(&self.pool)
            .await
            .map_err(|err| format!("list api keys failed: {err}"))?;
        rows.iter()
            .map(api_key_from_row)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| format!("decode api keys failed: {err}"))
    }

    async fn list_for_principal_page(
        &self,
        principal_id: &str,
        active_only: bool,
        now_unix: u64,
        limit: usize,
        offset: usize,
    ) -> Result<(Vec<ApiKeyRecord>, usize), String> {
        let rel = self.relation();
        let m = &self.model;
        let key_prefix_col = m.select("key_prefix");
        let key_hash = m.select("key_hash");
        let owner_id = m.select_as("owner_id", "principal_id");
        let owner_id_col = m.q("owner_id");
        let service_identity =
            m.json_get_as("metadata_json", "service_identity", "service_identity");
        let service_identity_expr = format!("{}->>'service_identity'", m.q("metadata_json"));
        let tenant_id = m.select("tenant_id");
        let project_id = m.select("project_id");
        let scopes = m.json_text_as("scopes_json", "scopes");
        let created_at_unix = m.timestamp_unix_as("created_at", "created_at_unix");
        let last_used_at_unix = m.timestamp_unix_as("last_used_at", "last_used_at_unix");
        let expires_at_unix = m.timestamp_unix_as("expires_at", "expires_at_unix");
        let revoked_at_unix = m.timestamp_unix_as("deleted_at", "revoked_at_unix");
        let deleted_at = m.q("deleted_at");
        let status_col = m.q("status");
        let expires_at = m.q("expires_at");
        let created_at = m.q("created_at");
        let active_clause = if active_only {
            format!(
                "AND {deleted_at} IS NULL AND {status_col} = 'ACTIVE' AND ({expires_at} IS NULL OR {expires_at} > to_timestamp($2::DOUBLE PRECISION))"
            )
        } else {
            String::new()
        };
        let count_sql = format!(
            "SELECT COUNT(*)::BIGINT AS total_count FROM {rel} WHERE ({owner_id_col} = $1 OR {service_identity_expr} = $1) {active_clause}"
        );
        let mut count_query = sqlx::query(&count_sql).bind(principal_id);
        if active_only {
            count_query = count_query.bind(now_unix as i64);
        }
        let total = count_query
            .fetch_one(&self.pool)
            .await
            .map_err(|err| format!("count api keys failed: {err}"))?
            .try_get::<i64, _>("total_count")
            .map_err(|err| format!("decode api key count failed: {err}"))?
            .max(0) as usize;
        let (limit_param, offset_param) = if active_only {
            ("$3", "$4")
        } else {
            ("$2", "$3")
        };
        let sql = format!(
            "SELECT {key_prefix_col}, {key_hash}, {owner_id}, {service_identity}, {tenant_id}, {project_id}, {scopes}, \
                    {created_at_unix}, {last_used_at_unix}, {expires_at_unix}, {revoked_at_unix} \
             FROM {rel} WHERE ({owner_id_col} = $1 OR {service_identity_expr} = $1) {active_clause} ORDER BY {created_at} DESC LIMIT {limit_param} OFFSET {offset_param}"
        );
        let mut query = sqlx::query(&sql).bind(principal_id);
        if active_only {
            query = query.bind(now_unix as i64);
        }
        let rows = query
            .bind(limit as i64)
            .bind(offset as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|err| format!("list api keys failed: {err}"))?;
        let items = rows
            .iter()
            .map(api_key_from_row)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| format!("decode api keys failed: {err}"))?;
        Ok((items, total))
    }

    async fn list_for_principal_status_page(
        &self,
        principal_id: &str,
        status: apikey_entity_pb::ApiKeyStatus,
        now_unix: u64,
        limit: usize,
        offset: usize,
    ) -> Result<(Vec<ApiKeyRecord>, usize), String> {
        let rel = self.relation();
        let m = &self.model;
        let key_prefix_col = m.select("key_prefix");
        let key_hash = m.select("key_hash");
        let owner_id = m.select_as("owner_id", "principal_id");
        let owner_id_col = m.q("owner_id");
        let service_identity =
            m.json_get_as("metadata_json", "service_identity", "service_identity");
        let service_identity_expr = format!("{}->>'service_identity'", m.q("metadata_json"));
        let tenant_id = m.select("tenant_id");
        let project_id = m.select("project_id");
        let scopes = m.json_text_as("scopes_json", "scopes");
        let created_at_unix = m.timestamp_unix_as("created_at", "created_at_unix");
        let last_used_at_unix = m.timestamp_unix_as("last_used_at", "last_used_at_unix");
        let expires_at_unix = m.timestamp_unix_as("expires_at", "expires_at_unix");
        let revoked_at_unix = m.timestamp_unix_as("deleted_at", "revoked_at_unix");
        let deleted_at = m.q("deleted_at");
        let status_col = m.q("status");
        let expires_at = m.q("expires_at");
        let created_at = m.q("created_at");
        let status_clause = match status {
            apikey_entity_pb::ApiKeyStatus::Unspecified => String::new(),
            apikey_entity_pb::ApiKeyStatus::Active => format!(
                "AND {deleted_at} IS NULL AND {status_col} = 'ACTIVE' AND ({expires_at} IS NULL OR {expires_at} > to_timestamp($2::DOUBLE PRECISION))"
            ),
            apikey_entity_pb::ApiKeyStatus::Revoked => {
                format!("AND ({deleted_at} IS NOT NULL OR {status_col} = 'REVOKED')")
            }
            apikey_entity_pb::ApiKeyStatus::Expired => format!(
                "AND {deleted_at} IS NULL AND ({status_col} = 'EXPIRED' OR ({status_col} = 'ACTIVE' AND {expires_at} IS NOT NULL AND {expires_at} <= to_timestamp($2::DOUBLE PRECISION)))"
            ),
        };
        let count_sql = format!(
            "SELECT COUNT(*)::BIGINT AS total_count FROM {rel} WHERE ({owner_id_col} = $1 OR {service_identity_expr} = $1) {status_clause}"
        );
        let mut count_query = sqlx::query(&count_sql).bind(principal_id);
        if matches!(
            status,
            apikey_entity_pb::ApiKeyStatus::Active | apikey_entity_pb::ApiKeyStatus::Expired
        ) {
            count_query = count_query.bind(now_unix as i64);
        }
        let total = count_query
            .fetch_one(&self.pool)
            .await
            .map_err(|err| format!("count api keys failed: {err}"))?
            .try_get::<i64, _>("total_count")
            .map_err(|err| format!("decode api key count failed: {err}"))?
            .max(0) as usize;
        let (limit_param, offset_param) = if matches!(
            status,
            apikey_entity_pb::ApiKeyStatus::Active | apikey_entity_pb::ApiKeyStatus::Expired
        ) {
            ("$3", "$4")
        } else {
            ("$2", "$3")
        };
        let sql = format!(
            "SELECT {key_prefix_col}, {key_hash}, {owner_id}, {service_identity}, {tenant_id}, {project_id}, {scopes}, \
                    {created_at_unix}, {last_used_at_unix}, {expires_at_unix}, {revoked_at_unix} \
             FROM {rel} WHERE ({owner_id_col} = $1 OR {service_identity_expr} = $1) {status_clause} ORDER BY {created_at} DESC LIMIT {limit_param} OFFSET {offset_param}"
        );
        let mut query = sqlx::query(&sql).bind(principal_id);
        if matches!(
            status,
            apikey_entity_pb::ApiKeyStatus::Active | apikey_entity_pb::ApiKeyStatus::Expired
        ) {
            query = query.bind(now_unix as i64);
        }
        let rows = query
            .bind(limit as i64)
            .bind(offset as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|err| format!("list api keys failed: {err}"))?;
        let items = rows
            .iter()
            .map(api_key_from_row)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| format!("decode api keys failed: {err}"))?;
        Ok((items, total))
    }

    async fn revoke(&self, key_prefix: &str, now_unix: u64) -> Result<bool, String> {
        let result = sqlx::query(&self.revoke_sql(1, 2))
            .bind(key_prefix)
            .bind(now_unix as i64)
            .execute(&self.pool)
            .await
            .map_err(|err| format!("revoke api key failed: {err}"))?;
        Ok(result.rows_affected() > 0)
    }

    async fn touch_last_used(&self, key_prefix: &str, now_unix: u64) -> Result<(), String> {
        let rel = self.relation();
        let m = &self.model;
        let last_used_at = m.q("last_used_at");
        let key_prefix_col = m.q("key_prefix");
        let deleted_at = m.q("deleted_at");
        sqlx::query(&format!(
            "UPDATE {rel} SET {last_used_at} = to_timestamp($2::DOUBLE PRECISION) \
             WHERE {key_prefix_col} = $1 AND {deleted_at} IS NULL"
        ))
        .bind(key_prefix)
        .bind(now_unix as i64)
        .execute(&self.pool)
        .await
        .map_err(|err| format!("touch api key last_used_at failed: {err}"))?;
        Ok(())
    }
}

pub struct PostgresUserStore {
    pool: PgPool,
    users_model: NativeModel,
    otps_model: NativeModel,
}

impl PostgresUserStore {
    pub fn new(pool: PgPool, schema: impl Into<String>) -> Self {
        let _ = schema.into();
        Self {
            pool,
            users_model: native_model(
                "udb.core.authn.entity.v1.User",
                &[
                    "user_id",
                    "username",
                    "email",
                    "password_hash",
                    "account_kind",
                    "status",
                    "tenant_id",
                    "full_name",
                    "totp_secret_enc",
                    "mfa_enabled",
                    "failed_login_count",
                    "locked_until",
                    "email_verified_at",
                    "last_login_at",
                    "created_by",
                    "created_at",
                    "updated_at",
                    "deleted_at",
                    "deleted_by",
                    "project_id",
                    "external_provider_id",
                    "external_subject",
                    "profile_attributes_json",
                ],
            ),
            otps_model: native_model(
                "udb.core.authn.entity.v1.OTP",
                &[
                    "otp_id",
                    "user_id",
                    "otp_type",
                    "code_hash",
                    "delivery_channel",
                    "delivery_address",
                    "status",
                    "attempt_count",
                    "superseded_by_id",
                    "expires_at",
                    "used_at",
                    "created_at",
                    "correlation_id",
                ],
            ),
        }
    }

    fn users_relation(&self) -> String {
        self.users_model.relation.clone()
    }

    fn otps_relation(&self) -> String {
        self.otps_model.relation.clone()
    }

    fn user_select_projection(&self) -> String {
        let m = &self.users_model;
        [
            m.text("user_id"),
            m.select("username"),
            m.select("email"),
            m.select("password_hash"),
            m.select("account_kind"),
            m.select("status"),
            m.select("tenant_id"),
            m.select("full_name"),
            m.text_or_empty_as("totp_secret_enc", "totp_secret_hash"),
            m.select("mfa_enabled"),
            m.select("failed_login_count"),
            uuid_or_empty_as_text(m, "created_by"),
            uuid_or_empty_as_text(m, "deleted_by"),
            m.text_or_empty("project_id"),
            m.text_or_empty("external_provider_id"),
            m.text_or_empty("external_subject"),
            m.json_text_as("profile_attributes_json", "profile_attributes_json"),
            m.timestamp_unix_as("locked_until", "locked_until_unix"),
            m.timestamp_unix_as("email_verified_at", "email_verified_at_unix"),
            m.timestamp_unix_as("last_login_at", "last_login_at_unix"),
            m.timestamp_unix_as("created_at", "created_at_unix"),
            m.timestamp_unix_as("updated_at", "updated_at_unix"),
            m.timestamp_unix_as("deleted_at", "deleted_at_unix"),
        ]
        .join(", ")
    }

    fn otp_select_projection(&self) -> String {
        let m = &self.otps_model;
        [
            m.text("otp_id"),
            m.text("user_id"),
            m.select("otp_type"),
            m.select("code_hash"),
            m.select("delivery_channel"),
            m.text_or_empty("delivery_address"),
            m.select("status"),
            m.select("attempt_count"),
            m.text_or_empty("superseded_by_id"),
            m.text_or_empty("correlation_id"),
            m.timestamp_unix_as("expires_at", "expires_at_unix"),
            m.timestamp_unix_as("used_at", "used_at_unix"),
            m.timestamp_unix_as("created_at", "created_at_unix"),
        ]
        .join(", ")
    }
}

#[async_trait]
impl UserStore for PostgresUserStore {
    async fn put_user(&self, record: UserRecord) -> Result<(), String> {
        let rel = self.users_relation();
        let m = &self.users_model;
        let user_id = m.q("user_id");
        let username = m.q("username");
        let email = m.q("email");
        let password_hash = m.q("password_hash");
        let account_kind = m.q("account_kind");
        let status = m.q("status");
        let tenant_id = m.q("tenant_id");
        let full_name = m.q("full_name");
        let totp_secret_enc = m.q("totp_secret_enc");
        let mfa_enabled = m.q("mfa_enabled");
        let failed_login_count = m.q("failed_login_count");
        let locked_until = m.q("locked_until");
        let email_verified_at = m.q("email_verified_at");
        let last_login_at = m.q("last_login_at");
        let created_by = m.q("created_by");
        let created_at = m.q("created_at");
        let updated_at = m.q("updated_at");
        let deleted_at = m.q("deleted_at");
        let deleted_by = m.q("deleted_by");
        let project_id = m.q("project_id");
        let external_provider_id = m.q("external_provider_id");
        let external_subject = m.q("external_subject");
        let profile_attributes_json = m.q("profile_attributes_json");
        sqlx::query(&format!(
            "INSERT INTO {rel} \
             ({user_id}, {username}, {email}, {password_hash}, {account_kind}, {status}, {tenant_id}, {full_name}, {totp_secret_enc}, {mfa_enabled}, {failed_login_count}, {locked_until}, {email_verified_at}, {last_login_at}, {created_by}, {created_at}, {updated_at}, {deleted_at}, {deleted_by}, {project_id}, {external_provider_id}, {external_subject}, {profile_attributes_json}) \
             VALUES ($1::UUID, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, \
                     CASE WHEN $12::BIGINT > 0 THEN to_timestamp($12::DOUBLE PRECISION) ELSE NULL END, \
                     CASE WHEN $13::BIGINT > 0 THEN to_timestamp($13::DOUBLE PRECISION) ELSE NULL END, \
                     CASE WHEN $14::BIGINT > 0 THEN to_timestamp($14::DOUBLE PRECISION) ELSE NULL END, \
                     NULLIF($15, '')::UUID, to_timestamp($16::DOUBLE PRECISION), to_timestamp($17::DOUBLE PRECISION), \
                     CASE WHEN $18::BIGINT > 0 THEN to_timestamp($18::DOUBLE PRECISION) ELSE NULL END, \
                     NULLIF($19, '')::UUID, $20, $21, $22, $23::JSONB) \
             ON CONFLICT ({user_id}) DO UPDATE SET \
               {username} = EXCLUDED.{username}, {email} = EXCLUDED.{email}, {password_hash} = EXCLUDED.{password_hash}, {account_kind} = EXCLUDED.{account_kind}, \
               {status} = EXCLUDED.{status}, {tenant_id} = EXCLUDED.{tenant_id}, {full_name} = EXCLUDED.{full_name}, {totp_secret_enc} = EXCLUDED.{totp_secret_enc}, \
               {mfa_enabled} = EXCLUDED.{mfa_enabled}, {failed_login_count} = EXCLUDED.{failed_login_count}, {locked_until} = EXCLUDED.{locked_until}, \
               {email_verified_at} = EXCLUDED.{email_verified_at}, {last_login_at} = EXCLUDED.{last_login_at}, {updated_at} = EXCLUDED.{updated_at}, \
               {deleted_at} = EXCLUDED.{deleted_at}, {deleted_by} = EXCLUDED.{deleted_by}, {project_id} = EXCLUDED.{project_id}, \
               {external_provider_id} = EXCLUDED.{external_provider_id}, {external_subject} = EXCLUDED.{external_subject}, {profile_attributes_json} = EXCLUDED.{profile_attributes_json}"
        ))
        .bind(&record.user_id)
        .bind(&record.username)
        .bind(&record.email)
        .bind(&record.password_hash)
        .bind(account_kind_to_db(record.account_kind))
        .bind(user_status_to_db(record.status))
        .bind(&record.tenant_id)
        .bind(&record.full_name)
        .bind(&record.totp_secret_hash)
        .bind(record.mfa_enabled)
        .bind(record.failed_login_count)
        .bind(record.locked_until_unix as i64)
        .bind(record.email_verified_at_unix as i64)
        .bind(record.last_login_at_unix as i64)
        .bind(&record.created_by)
        .bind(record.created_at_unix as i64)
        .bind(record.updated_at_unix as i64)
        .bind(record.deleted_at_unix as i64)
        .bind(&record.deleted_by)
        .bind(&record.project_id)
        .bind(&record.external_provider_id)
        .bind(&record.external_subject)
        .bind(json_object_or_empty(&record.profile_attributes_json))
        .execute(&self.pool)
        .await
        .map_err(|err| format!("put user failed: {err}"))?;
        Ok(())
    }

    async fn get_user_by_id(&self, user_id: &str) -> Result<Option<UserRecord>, String> {
        self.get_user_by("user_id", user_id).await
    }

    async fn get_user_by_username(&self, username: &str) -> Result<Option<UserRecord>, String> {
        self.get_user_by("username", username).await
    }

    async fn get_user_by_email(&self, email: &str) -> Result<Option<UserRecord>, String> {
        self.get_user_by("email", email).await
    }

    async fn list_users(
        &self,
        tenant_id: &str,
        account_kind: i32,
        status: i32,
    ) -> Result<Vec<UserRecord>, String> {
        let rel = self.users_relation();
        let projection = self.user_select_projection();
        let tenant_id_col = self.users_model.q("tenant_id");
        let account_kind_col = self.users_model.q("account_kind");
        let status_col = self.users_model.q("status");
        let created_at = self.users_model.q("created_at");
        let deleted_at = self.users_model.q("deleted_at");
        let account_kind_filter = account_kind_to_db(account_kind);
        let status_filter = user_status_to_db(status);
        let rows = sqlx::query(&format!(
            "SELECT {projection} \
             FROM {rel} WHERE {deleted_at} IS NULL AND ($1 = '' OR {tenant_id_col} = $1) AND ($2 = 'UNSPECIFIED' OR {account_kind_col} = $2) AND ($3 = 'UNSPECIFIED' OR {status_col} = $3) ORDER BY {created_at} DESC"
        ))
        .bind(tenant_id)
        .bind(account_kind_filter)
        .bind(status_filter)
        .fetch_all(&self.pool)
        .await
        .map_err(|err| format!("list users failed: {err}"))?;
        rows.iter()
            .map(user_from_row)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| format!("decode users failed: {err}"))
    }

    async fn list_users_page(
        &self,
        tenant_id: &str,
        account_kind: i32,
        status: i32,
        limit: usize,
        offset: usize,
    ) -> Result<(Vec<UserRecord>, usize), String> {
        let rel = self.users_relation();
        let projection = self.user_select_projection();
        let tenant_id_col = self.users_model.q("tenant_id");
        let account_kind_col = self.users_model.q("account_kind");
        let status_col = self.users_model.q("status");
        let created_at = self.users_model.q("created_at");
        let deleted_at = self.users_model.q("deleted_at");
        let account_kind_filter = account_kind_to_db(account_kind);
        let status_filter = user_status_to_db(status);
        let where_clause = format!(
            "{deleted_at} IS NULL AND ($1 = '' OR {tenant_id_col} = $1) AND ($2 = 'UNSPECIFIED' OR {account_kind_col} = $2) AND ($3 = 'UNSPECIFIED' OR {status_col} = $3)"
        );
        let total = sqlx::query(&format!(
            "SELECT COUNT(*)::BIGINT AS total_count FROM {rel} WHERE {where_clause}"
        ))
        .bind(tenant_id)
        .bind(&account_kind_filter)
        .bind(&status_filter)
        .fetch_one(&self.pool)
        .await
        .map_err(|err| format!("count users failed: {err}"))?
        .try_get::<i64, _>("total_count")
        .map_err(|err| format!("decode user count failed: {err}"))?
        .max(0) as usize;
        let rows = sqlx::query(&format!(
            "SELECT {projection} FROM {rel} WHERE {where_clause} ORDER BY {created_at} DESC LIMIT $4 OFFSET $5"
        ))
        .bind(tenant_id)
        .bind(account_kind_filter)
        .bind(status_filter)
        .bind(limit as i64)
        .bind(offset as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|err| format!("list users failed: {err}"))?;
        let items = rows
            .iter()
            .map(user_from_row)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| format!("decode users failed: {err}"))?;
        Ok((items, total))
    }

    async fn delete_user(
        &self,
        user_id: &str,
        deleted_by: &str,
        now_unix: u64,
    ) -> Result<bool, String> {
        let rel = self.users_relation();
        let user_id_col = self.users_model.q("user_id");
        let deleted_at = self.users_model.q("deleted_at");
        let deleted_by_col = self.users_model.q("deleted_by");
        let updated_at = self.users_model.q("updated_at");
        let result = sqlx::query(&format!(
            "UPDATE {rel} SET {deleted_at} = to_timestamp($2::DOUBLE PRECISION), {deleted_by_col} = NULLIF($3, '')::UUID, {updated_at} = to_timestamp($2::DOUBLE PRECISION) WHERE {user_id_col} = $1::UUID AND {deleted_at} IS NULL"
        ))
        .bind(user_id)
        .bind(now_unix as i64)
        .bind(deleted_by)
        .execute(&self.pool)
        .await
        .map_err(|err| format!("delete user failed: {err}"))?;
        Ok(result.rows_affected() > 0)
    }

    async fn put_otp(&self, record: OtpRecord) -> Result<(), String> {
        let rel = self.otps_relation();
        let m = &self.otps_model;
        let otp_id = m.q("otp_id");
        let user_id = m.q("user_id");
        let otp_type = m.q("otp_type");
        let code_hash = m.q("code_hash");
        let delivery_channel = m.q("delivery_channel");
        let delivery_address = m.q("delivery_address");
        let status = m.q("status");
        let attempt_count = m.q("attempt_count");
        let superseded_by_id = m.q("superseded_by_id");
        let expires_at = m.q("expires_at");
        let used_at = m.q("used_at");
        let created_at = m.q("created_at");
        let correlation_id = m.q("correlation_id");
        sqlx::query(&format!(
            "INSERT INTO {rel} \
             ({otp_id}, {user_id}, {otp_type}, {code_hash}, {delivery_channel}, {delivery_address}, {status}, {attempt_count}, {superseded_by_id}, {expires_at}, {used_at}, {created_at}, {correlation_id}) \
             VALUES ($1::UUID, $2::UUID, $3, $4, $5, $6, $7, $8, NULLIF($9, '')::UUID, to_timestamp($10::DOUBLE PRECISION), \
                     CASE WHEN $11::BIGINT > 0 THEN to_timestamp($11::DOUBLE PRECISION) ELSE NULL END, to_timestamp($12::DOUBLE PRECISION), $13) \
             ON CONFLICT ({otp_id}) DO UPDATE SET {status} = EXCLUDED.{status}, {attempt_count} = EXCLUDED.{attempt_count}, {superseded_by_id} = EXCLUDED.{superseded_by_id}, {used_at} = EXCLUDED.{used_at}"
        ))
        .bind(&record.otp_id)
        .bind(&record.user_id)
        .bind(otp_type_to_db(record.otp_type))
        .bind(&record.code_hash)
        .bind(&record.delivery_channel)
        .bind(&record.delivery_address)
        .bind(otp_status_to_db(record.status))
        .bind(record.attempt_count)
        .bind(&record.superseded_by_id)
        .bind(record.expires_at_unix as i64)
        .bind(record.used_at_unix as i64)
        .bind(record.created_at_unix as i64)
        .bind(&record.correlation_id)
        .execute(&self.pool)
        .await
        .map_err(|err| format!("put otp failed: {err}"))?;
        Ok(())
    }

    async fn get_otp(&self, otp_id: &str) -> Result<Option<OtpRecord>, String> {
        let rel = self.otps_relation();
        let projection = self.otp_select_projection();
        let otp_id_col = self.otps_model.q("otp_id");
        let row = sqlx::query(&format!(
            "SELECT {projection} FROM {rel} WHERE {otp_id_col} = $1::UUID"
        ))
        .bind(otp_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| format!("get otp failed: {err}"))?;
        row.as_ref()
            .map(otp_from_row)
            .transpose()
            .map_err(|err| format!("decode otp failed: {err}"))
    }

    async fn update_otp(&self, record: OtpRecord) -> Result<(), String> {
        self.put_otp(record).await
    }
}

impl PostgresUserStore {
    async fn get_user_by(&self, column: &str, value: &str) -> Result<Option<UserRecord>, String> {
        let rel = self.users_relation();
        let col = self.users_model.q(column);
        let projection = self.user_select_projection();
        let deleted_at = self.users_model.q("deleted_at");
        let value_expr = if column == "user_id" {
            "$1::UUID"
        } else {
            "$1"
        };
        let row = sqlx::query(&format!(
            "SELECT {projection} FROM {rel} WHERE {col} = {value_expr} AND {deleted_at} IS NULL"
        ))
        .bind(value)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| format!("get user failed: {err}"))?;
        row.as_ref()
            .map(user_from_row)
            .transpose()
            .map_err(|err| format!("decode user failed: {err}"))
    }
}

/// Validate a raw session id against the store at `now_unix`, returning the
/// record only when it exists and is active. Hashing keeps raw ids out of storage.
pub async fn validate_session(
    store: &dyn SessionStore,
    raw_session_id: &str,
    hash_key: &[u8],
    now_unix: u64,
    idle_ttl_secs: u64,
) -> Result<Option<SessionRecord>, String> {
    let hash = hash_secret(raw_session_id, hash_key);
    let Some(mut rec) = store.get(&hash).await? else {
        return Ok(None);
    };
    if !rec.is_active_with_idle(now_unix, idle_ttl_secs) {
        return Ok(None);
    }
    rec.updated_at_unix = now_unix;
    store.touch_last_active(&hash, now_unix).await?;
    Ok(Some(rec))
}

/// Refresh an active session: extend its absolute expiry to `now_unix +
/// ttl_secs` and stamp `updated_at`. Returns the refreshed record, or `None`
/// when the session is missing or already inactive (expired/revoked sessions
/// cannot be revived).
pub async fn refresh_session(
    store: &dyn SessionStore,
    raw_session_id: &str,
    hash_key: &[u8],
    now_unix: u64,
    ttl_secs: u64,
) -> Result<Option<SessionRecord>, String> {
    let hash = hash_secret(raw_session_id, hash_key);
    let Some(mut rec) = store.get(&hash).await? else {
        return Ok(None);
    };
    if !rec.is_active(now_unix) {
        return Ok(None);
    }
    rec.updated_at_unix = now_unix;
    rec.expires_at_unix = now_unix.saturating_add(ttl_secs);
    // Borrow for the write — only timestamps changed; no need to clone the whole
    // record just to return it (#104).
    store.put(&rec).await?;
    Ok(Some(rec))
}

/// Validate a raw API key (`<prefix>.<secret>`) against the store: prefix lookup
/// then constant-form hash compare, returning the record only when active.
pub async fn validate_api_key(
    store: &dyn ApiKeyStore,
    raw_key: &str,
    hash_key: &[u8],
    now_unix: u64,
) -> Result<Option<ApiKeyRecord>, String> {
    let prefix = api_key_prefix(raw_key);
    let Some(rec) = store.get_by_prefix(&prefix).await? else {
        return Ok(None);
    };
    if !constant_time_eq(&rec.key_hash, &hash_secret(raw_key, hash_key)) {
        return Ok(None);
    }
    Ok(rec.is_active(now_unix).then_some(rec))
}

/// Rotate an API key: store `new_raw_key` (carrying the principal/tenant/scopes
/// from `template`) and revoke the old key by prefix. Returns the new record.
/// The two raw keys are never stored — only their `hash_secret` digests.
pub async fn rotate_api_key(
    store: &dyn ApiKeyStore,
    old_key_prefix: &str,
    new_raw_key: &str,
    hash_key: &[u8],
    now_unix: u64,
    template: ApiKeyRecord,
) -> Result<ApiKeyRecord, String> {
    let mut new_rec = template;
    new_rec.key_prefix = api_key_prefix(new_raw_key);
    new_rec.key_hash = hash_secret(new_raw_key, hash_key);
    new_rec.created_at_unix = now_unix;
    new_rec.revoked_at_unix = 0;
    // #209: atomic insert-new + revoke-old (single transaction in the Postgres
    // store) so a revoke failure can't leave both keys active. Avoids the prior
    // `put(clone())` + separate `revoke()` split-brain window.
    store
        .rotate(old_key_prefix, new_rec.clone(), now_unix)
        .await?;
    Ok(new_rec)
}

/// Constant-time equality for secret digests — avoids the byte-wise early-exit
/// timing leak of `==`. In practice both inputs are fixed-length HMAC hex
/// digests, so a length mismatch (only on a corrupt/absent stored hash) returns
/// false without revealing which position differed.
pub fn constant_time_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Argon2id hasher peppered with the deployment hash key (used as the Argon2
/// "secret" parameter). The pepper means a leaked password-hash column alone
/// cannot be brute-forced offline without also stealing the server secret.
fn argon2_with_pepper(hash_key: &[u8]) -> argon2::Argon2<'_> {
    if hash_key.is_empty() {
        argon2::Argon2::default()
    } else {
        argon2::Argon2::new_with_secret(
            hash_key,
            argon2::Algorithm::Argon2id,
            argon2::Version::V0x13,
            argon2::Params::default(),
        )
        .unwrap_or_else(|_| argon2::Argon2::default())
    }
}

/// Hash a password with Argon2id (PHC string, self-describing salt + params).
/// Replaces the previous keyed-HMAC scheme; [`verify_password`] still accepts
/// legacy `hmac-sha256:` hashes so existing credentials keep working and are
/// transparently re-hashed on the next password change.
pub fn hash_password(password: &str, hash_key: &[u8]) -> String {
    use argon2::PasswordHasher;
    use argon2::password_hash::SaltString;

    // A random per-password salt from UUIDv4 entropy (no extra RNG dependency).
    let salt_bytes = *uuid::Uuid::new_v4().as_bytes();
    match SaltString::encode_b64(&salt_bytes) {
        Ok(salt) => match argon2_with_pepper(hash_key).hash_password(password.as_bytes(), &salt) {
            Ok(hash) => hash.to_string(),
            // Argon2 only errors on pathological params; fall back to the keyed
            // HMAC so account creation never hard-fails on a hashing hiccup.
            Err(_) => hash_secret(&format!("password:{password}"), hash_key),
        },
        Err(_) => hash_secret(&format!("password:{password}"), hash_key),
    }
}

/// Verify a password against either an Argon2id PHC hash (new) or a legacy
/// keyed-HMAC hash (`hmac-sha256:`), so the KDF upgrade is non-breaking.
pub fn verify_password(password: &str, hash_key: &[u8], stored_hash: &str) -> bool {
    use argon2::PasswordVerifier;
    use argon2::password_hash::PasswordHash;

    if stored_hash.is_empty() {
        return false;
    }
    if stored_hash.starts_with("$argon2") {
        return match PasswordHash::new(stored_hash) {
            Ok(parsed) => argon2_with_pepper(hash_key)
                .verify_password(password.as_bytes(), &parsed)
                .is_ok(),
            Err(_) => false,
        };
    }
    // Legacy keyed-HMAC fallback (constant-time) for pre-Argon2 stored hashes.
    constant_time_eq(
        &hash_secret(&format!("password:{password}"), hash_key),
        stored_hash,
    )
}

/// True when a stored hash uses the legacy keyed-HMAC scheme and should be
/// re-hashed with Argon2id on the next successful authentication.
pub fn password_hash_needs_upgrade(stored_hash: &str) -> bool {
    !stored_hash.is_empty() && !stored_hash.starts_with("$argon2")
}

pub fn hash_otp_code(code: &str, hash_key: &[u8]) -> String {
    hash_secret(&format!("otp:{}", code.trim()), hash_key)
}

pub fn verify_otp_code(code: &str, hash_key: &[u8], stored_hash: &str) -> bool {
    !stored_hash.is_empty() && constant_time_eq(&hash_otp_code(code, hash_key), stored_hash)
}

#[cfg(test)]
mod tests;
