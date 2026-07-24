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

use crate::backend::BackendKind;
use crate::ir::compile::CompileContext;
use crate::ir::{
    ComparisonOp, ConflictStrategy, LogicalAssignment, LogicalDelete, LogicalFilter, LogicalRecord,
    LogicalUpdate, LogicalValue, LogicalWrite,
};
use crate::runtime::security::hmac_sha256;

use crate::runtime::authz::Principal;
use crate::runtime::native_catalog::{NativeModel, native_model};

/// RFC 6238 TOTP for native MFA (enrollment, verification, secret-at-rest).
pub mod mfa_challenge;
pub mod profile;
pub mod revocation;
pub mod signing_keys;
pub mod token_family;
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
#[derive(Clone, PartialEq, Eq)]
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

// Manual `Debug` redacting the four keyed-HMAC secrets. Session/OTP timing and
// backend selection are non-secret operational settings and print normally.
impl std::fmt::Debug for AuthnConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthnConfig")
            .field("session_enabled", &self.session_enabled)
            .field("session_backend", &self.session_backend)
            .field("session_ttl_secs", &self.session_ttl_secs)
            .field("session_idle_ttl_secs", &self.session_idle_ttl_secs)
            .field("session_header", &self.session_header)
            .field("session_cookie", &self.session_cookie)
            .field("session_hash_secret", &"[redacted]")
            .field("api_key_hash_secret", &"[redacted]")
            .field("password_hash_secret", &"[redacted]")
            .field("otp_hash_secret", &"[redacted]")
            .field("otp_ttl_secs", &self.otp_ttl_secs)
            .field("otp_cooldown_secs", &self.otp_cooldown_secs)
            .finish()
    }
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
    pub name: String,
    pub description: String,
    pub principal_id: String,
    pub service_identity: String,
    pub tenant_id: String,
    pub project_id: String,
    pub scopes: Vec<String>,
    /// Typed service-account grant revision reviewed when this key was issued
    /// or explicitly updated. Stored in `metadata_json` for compatibility with
    /// the existing API-key table contract.
    pub grant_revision: i64,
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

/// Domain-owned principal category for a native user account.
///
/// This mirrors the proto `udb.core.authn.entity.v1.AccountKind` *values* but is
/// owned by the domain engine and carries no prost/proto dependency — the
/// domain must not store proto enum integers (final_task.md §7 "Move proto enum
/// conversions out of domain engines"). Discriminants match the proto integer
/// values so the adapter's i32 conversion is a 1:1 cast, but that coupling is the
/// adapter's concern, not the domain's. DB persistence uses [`AccountKind::to_db`]
/// / [`AccountKind::from_db`], not the integer.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[repr(i32)]
pub enum AccountKind {
    #[default]
    Unspecified = 0,
    Person = 1,
    ServiceAccount = 2,
    Workload = 3,
    ExternalIdentity = 4,
    System = 5,
    Anonymous = 6,
}

impl AccountKind {
    /// Canonical upper-snake DB string (matches the persisted column values).
    pub fn to_db(self) -> &'static str {
        match self {
            AccountKind::Unspecified => "UNSPECIFIED",
            AccountKind::Person => "PERSON",
            AccountKind::ServiceAccount => "SERVICE_ACCOUNT",
            AccountKind::Workload => "WORKLOAD",
            AccountKind::ExternalIdentity => "EXTERNAL_IDENTITY",
            AccountKind::System => "SYSTEM",
            AccountKind::Anonymous => "ANONYMOUS",
        }
    }

    /// Parse the DB string, accepting both the bare (`"PERSON"`) and the
    /// proto-prefixed (`"ACCOUNT_KIND_PERSON"`) spellings; unknown → `Unspecified`.
    pub fn from_db(value: &str) -> Self {
        match value.strip_prefix("ACCOUNT_KIND_").unwrap_or(value) {
            "PERSON" => AccountKind::Person,
            "SERVICE_ACCOUNT" => AccountKind::ServiceAccount,
            "WORKLOAD" => AccountKind::Workload,
            "EXTERNAL_IDENTITY" => AccountKind::ExternalIdentity,
            "SYSTEM" => AccountKind::System,
            "ANONYMOUS" => AccountKind::Anonymous,
            _ => AccountKind::Unspecified,
        }
    }

    /// The raw enum integer. Equal to the proto value, but this is a plain
    /// integer accessor — the *proto* conversion lives in the service adapter.
    pub fn as_i32(self) -> i32 {
        self as i32
    }

    /// Build from a raw enum integer; unknown values map to `Unspecified`.
    pub fn from_i32(value: i32) -> Self {
        match value {
            1 => AccountKind::Person,
            2 => AccountKind::ServiceAccount,
            3 => AccountKind::Workload,
            4 => AccountKind::ExternalIdentity,
            5 => AccountKind::System,
            6 => AccountKind::Anonymous,
            _ => AccountKind::Unspecified,
        }
    }
}

/// Domain-owned account lifecycle state for a native user account.
///
/// Mirrors the proto `udb.core.authn.entity.v1.UserStatus` values without any
/// proto dependency (see [`AccountKind`] for the rationale). DB persistence uses
/// [`AccountStatus::to_db`] / [`AccountStatus::from_db`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[repr(i32)]
pub enum AccountStatus {
    #[default]
    Unspecified = 0,
    PendingVerification = 1,
    Active = 2,
    Suspended = 3,
    Locked = 4,
    Deactivated = 5,
}

impl AccountStatus {
    pub fn to_db(self) -> &'static str {
        match self {
            AccountStatus::Unspecified => "UNSPECIFIED",
            AccountStatus::PendingVerification => "PENDING_VERIFICATION",
            AccountStatus::Active => "ACTIVE",
            AccountStatus::Suspended => "SUSPENDED",
            AccountStatus::Locked => "LOCKED",
            AccountStatus::Deactivated => "DEACTIVATED",
        }
    }

    pub fn from_db(value: &str) -> Self {
        match value.strip_prefix("USER_STATUS_").unwrap_or(value) {
            "PENDING_VERIFICATION" => AccountStatus::PendingVerification,
            "ACTIVE" => AccountStatus::Active,
            "SUSPENDED" => AccountStatus::Suspended,
            "LOCKED" => AccountStatus::Locked,
            "DEACTIVATED" => AccountStatus::Deactivated,
            _ => AccountStatus::Unspecified,
        }
    }

    pub fn as_i32(self) -> i32 {
        self as i32
    }

    pub fn from_i32(value: i32) -> Self {
        match value {
            1 => AccountStatus::PendingVerification,
            2 => AccountStatus::Active,
            3 => AccountStatus::Suspended,
            4 => AccountStatus::Locked,
            5 => AccountStatus::Deactivated,
            _ => AccountStatus::Unspecified,
        }
    }

    pub fn is_active(self) -> bool {
        matches!(self, AccountStatus::Active)
    }
}

/// Domain-owned one-time-password purpose.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[repr(i32)]
pub enum OtpType {
    #[default]
    Unspecified = 0,
    EmailVerification = 1,
    Login2fa = 2,
    PasswordReset = 3,
    SensitiveOperation = 4,
    PhoneVerification = 5,
}

impl OtpType {
    pub fn to_db(self) -> &'static str {
        match self {
            OtpType::Unspecified => "UNSPECIFIED",
            OtpType::EmailVerification => "EMAIL_VERIFICATION",
            OtpType::Login2fa => "LOGIN_2FA",
            OtpType::PasswordReset => "PASSWORD_RESET",
            OtpType::SensitiveOperation => "SENSITIVE_OPERATION",
            OtpType::PhoneVerification => "PHONE_VERIFICATION",
        }
    }

    pub fn from_db(value: &str) -> Self {
        match value.strip_prefix("OTP_TYPE_").unwrap_or(value) {
            "EMAIL_VERIFICATION" => OtpType::EmailVerification,
            "LOGIN_2FA" => OtpType::Login2fa,
            "PASSWORD_RESET" => OtpType::PasswordReset,
            "SENSITIVE_OPERATION" => OtpType::SensitiveOperation,
            "PHONE_VERIFICATION" => OtpType::PhoneVerification,
            _ => OtpType::Unspecified,
        }
    }

    pub fn as_i32(self) -> i32 {
        self as i32
    }

    pub fn from_i32(value: i32) -> Self {
        match value {
            1 => OtpType::EmailVerification,
            2 => OtpType::Login2fa,
            3 => OtpType::PasswordReset,
            4 => OtpType::SensitiveOperation,
            5 => OtpType::PhoneVerification,
            _ => OtpType::Unspecified,
        }
    }
}

/// Domain-owned one-time-password lifecycle state.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[repr(i32)]
pub enum OtpStatus {
    #[default]
    Unspecified = 0,
    Pending = 1,
    Used = 2,
    Expired = 3,
    Invalidated = 4,
}

impl OtpStatus {
    pub fn to_db(self) -> &'static str {
        match self {
            OtpStatus::Unspecified => "UNSPECIFIED",
            OtpStatus::Pending => "PENDING",
            OtpStatus::Used => "USED",
            OtpStatus::Expired => "EXPIRED",
            OtpStatus::Invalidated => "INVALIDATED",
        }
    }

    pub fn from_db(value: &str) -> Self {
        match value.strip_prefix("OTP_STATUS_").unwrap_or(value) {
            "PENDING" => OtpStatus::Pending,
            "USED" => OtpStatus::Used,
            "EXPIRED" => OtpStatus::Expired,
            "INVALIDATED" => OtpStatus::Invalidated,
            _ => OtpStatus::Unspecified,
        }
    }

    pub fn as_i32(self) -> i32 {
        self as i32
    }

    pub fn from_i32(value: i32) -> Self {
        match value {
            1 => OtpStatus::Pending,
            2 => OtpStatus::Used,
            3 => OtpStatus::Expired,
            4 => OtpStatus::Invalidated,
            _ => OtpStatus::Unspecified,
        }
    }
}

/// Domain-owned API-key lifecycle state.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[repr(i32)]
pub enum ApiKeyStatus {
    #[default]
    Unspecified = 0,
    Active = 1,
    Revoked = 2,
    Expired = 3,
}

impl ApiKeyStatus {
    pub fn to_db(self) -> &'static str {
        match self {
            ApiKeyStatus::Unspecified => "UNSPECIFIED",
            ApiKeyStatus::Active => "ACTIVE",
            ApiKeyStatus::Revoked => "REVOKED",
            ApiKeyStatus::Expired => "EXPIRED",
        }
    }

    pub fn from_i32(value: i32) -> Self {
        match value {
            1 => ApiKeyStatus::Active,
            2 => ApiKeyStatus::Revoked,
            3 => ApiKeyStatus::Expired,
            _ => ApiKeyStatus::Unspecified,
        }
    }

    pub fn as_i32(self) -> i32 {
        self as i32
    }
}

/// Native user account record stored by Stage-1 authn.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct UserRecord {
    pub user_id: String,
    pub username: String,
    pub email: String,
    pub password_hash: String,
    /// Domain-owned principal category (no proto/prost dependency). The adapter
    /// converts to/from the proto `AccountKind` enum at the service boundary.
    pub account_kind: AccountKind,
    /// Domain-owned account lifecycle state (no proto/prost dependency). The
    /// adapter converts to/from the proto `UserStatus` enum at the boundary.
    pub status: AccountStatus,
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

// Manual `Debug` redacting the password verifier (`password_hash`) and the
// encrypted/hashed TOTP secret (`totp_secret_hash`). These are the same
// credential fields the outbound `user_record_to_pb` mapper blanks; here the
// guard extends to internal `{:?}` logging. Identity/audit fields print
// normally for debuggability.
impl std::fmt::Debug for UserRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UserRecord")
            .field("user_id", &self.user_id)
            .field("username", &self.username)
            .field("email", &self.email)
            .field("password_hash", &"[redacted]")
            .field("account_kind", &self.account_kind)
            .field("status", &self.status)
            .field("tenant_id", &self.tenant_id)
            .field("full_name", &self.full_name)
            .field("totp_secret_hash", &"[redacted]")
            .field("mfa_enabled", &self.mfa_enabled)
            .field("failed_login_count", &self.failed_login_count)
            .field("locked_until_unix", &self.locked_until_unix)
            .field("email_verified_at_unix", &self.email_verified_at_unix)
            .field("last_login_at_unix", &self.last_login_at_unix)
            .field("created_by", &self.created_by)
            .field("created_at_unix", &self.created_at_unix)
            .field("updated_at_unix", &self.updated_at_unix)
            .field("deleted_at_unix", &self.deleted_at_unix)
            .field("deleted_by", &self.deleted_by)
            .field("project_id", &self.project_id)
            .field("external_provider_id", &self.external_provider_id)
            .field("external_subject", &self.external_subject)
            .field("profile_attributes_json", &self.profile_attributes_json)
            .finish()
    }
}

/// One-time password / step-up challenge record.
#[derive(Clone, Default, PartialEq, Eq)]
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
    /// Tenant boundary, denormalized from the owning user. Empty when unknown
    /// (column is nullable; read tolerates NULL).
    pub tenant_id: String,
}

// Manual `Debug` redacting the one-time-code verifier (`code_hash`). The
// delivery channel/address and timing fields remain visible for debugging.
impl std::fmt::Debug for OtpRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OtpRecord")
            .field("otp_id", &self.otp_id)
            .field("user_id", &self.user_id)
            .field("otp_type", &self.otp_type)
            .field("code_hash", &"[redacted]")
            .field("delivery_channel", &self.delivery_channel)
            .field("delivery_address", &self.delivery_address)
            .field("status", &self.status)
            .field("attempt_count", &self.attempt_count)
            .field("superseded_by_id", &self.superseded_by_id)
            .field("expires_at_unix", &self.expires_at_unix)
            .field("used_at_unix", &self.used_at_unix)
            .field("created_at_unix", &self.created_at_unix)
            .field("correlation_id", &self.correlation_id)
            .field("tenant_id", &self.tenant_id)
            .finish()
    }
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

fn otp_type_to_db(value: i32) -> &'static str {
    OtpType::from_i32(value).to_db()
}

fn otp_type_from_db(value: &str) -> i32 {
    OtpType::from_db(value).as_i32()
}

fn otp_status_to_db(value: i32) -> &'static str {
    OtpStatus::from_i32(value).to_db()
}

fn otp_status_from_db(value: &str) -> i32 {
    OtpStatus::from_db(value).as_i32()
}

fn api_key_status_to_db(status: ApiKeyStatus) -> &'static str {
    status.to_db()
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
        name: row_string(row, "name")?,
        description: row_string(row, "description")?,
        principal_id: row.try_get("principal_id")?,
        service_identity: row_string(row, "service_identity")?,
        tenant_id: row_string(row, "tenant_id")?,
        project_id: row_string(row, "project_id")?,
        scopes: string_list_from_json(&row.try_get::<String, _>("scopes")?),
        grant_revision: row_string(row, "grant_revision")?
            .parse::<i64>()
            .unwrap_or_default(),
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
        account_kind: AccountKind::from_db(&row.try_get::<String, _>("account_kind")?),
        status: AccountStatus::from_db(&row.try_get::<String, _>("status")?),
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
        tenant_id: row_string(row, "tenant_id")?,
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
    /// Revoke a principal's sessions on an existing transaction connection, so the
    /// revoke commits atomically with the caller's other writes (audit event etc.).
    /// Default: non-atomic `revoke_all_for_principal` for non-transactional stores.
    async fn revoke_all_for_principal_in_tx(
        &self,
        _conn: &mut sqlx::PgConnection,
        principal_id: &str,
        now_unix: u64,
    ) -> Result<usize, String> {
        self.revoke_all_for_principal(principal_id, now_unix).await
    }
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
        status: ApiKeyStatus,
        now_unix: u64,
        limit: usize,
        offset: usize,
    ) -> Result<(Vec<ApiKeyRecord>, usize), String> {
        let mut all = self
            .list_for_principal(principal_id, false, now_unix)
            .await?;
        match status {
            ApiKeyStatus::Unspecified => {}
            ApiKeyStatus::Active => {
                all.retain(|rec| !rec.is_revoked() && !rec.is_expired(now_unix));
            }
            ApiKeyStatus::Revoked => all.retain(ApiKeyRecord::is_revoked),
            ApiKeyStatus::Expired => {
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

    /// Persist `record` on an existing transaction connection so it commits
    /// atomically with the caller's other writes (e.g. an audit/outbox event).
    /// Default: non-atomic `put_user` for stores without a transactional impl.
    async fn put_user_in_tx(
        &self,
        _conn: &mut sqlx::PgConnection,
        record: UserRecord,
    ) -> Result<(), String> {
        self.put_user(record).await
    }

    /// The backing Postgres pool when this store is Postgres-backed, so a service
    /// built from stores alone can run the multi-statement transactions the atomic
    /// paths need (e.g. `create_user`'s user+OTP+outbox-event commit) WITHOUT a
    /// separately-wired `.with_postgres()`. Default `None` for non-Postgres stores.
    fn backing_pool(&self) -> Option<PgPool> {
        None
    }
    async fn get_user_by_id(&self, user_id: &str) -> Result<Option<UserRecord>, String>;
    async fn get_user_by_username(&self, username: &str) -> Result<Option<UserRecord>, String>;
    async fn get_user_by_email(&self, email: &str) -> Result<Option<UserRecord>, String>;
    async fn get_user_by_id_in_tenant(
        &self,
        user_id: &str,
        tenant_id: &str,
    ) -> Result<Option<UserRecord>, String> {
        let tenant_id = tenant_id.trim();
        if tenant_id.is_empty() {
            return Ok(None);
        }
        Ok(self
            .get_user_by_id(user_id)
            .await?
            .filter(|rec| rec.tenant_id == tenant_id))
    }
    async fn get_user_by_username_in_tenant(
        &self,
        username: &str,
        tenant_id: &str,
    ) -> Result<Option<UserRecord>, String> {
        let tenant_id = tenant_id.trim();
        if tenant_id.is_empty() {
            return Ok(None);
        }
        Ok(self
            .get_user_by_username(username)
            .await?
            .filter(|rec| rec.tenant_id == tenant_id))
    }
    async fn get_user_by_email_in_tenant(
        &self,
        email: &str,
        tenant_id: &str,
    ) -> Result<Option<UserRecord>, String> {
        let tenant_id = tenant_id.trim();
        if tenant_id.is_empty() {
            return Ok(None);
        }
        Ok(self
            .get_user_by_email(email)
            .await?
            .filter(|rec| rec.tenant_id == tenant_id))
    }
    async fn list_users(
        &self,
        tenant_id: &str,
        account_kind: AccountKind,
        status: AccountStatus,
    ) -> Result<Vec<UserRecord>, String>;
    async fn list_users_page(
        &self,
        tenant_id: &str,
        account_kind: AccountKind,
        status: AccountStatus,
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

    /// Persist `record` on an existing transaction connection so the OTP row
    /// commits atomically with the caller's other writes (e.g. the `create_user`
    /// upsert + its audit/outbox event). Default: non-atomic `put_otp` for stores
    /// without a transactional impl.
    async fn put_otp_in_tx(
        &self,
        _conn: &mut sqlx::PgConnection,
        record: OtpRecord,
    ) -> Result<(), String> {
        self.put_otp(record).await
    }
    async fn get_otp(&self, otp_id: &str) -> Result<Option<OtpRecord>, String>;
    async fn update_otp(&self, record: OtpRecord) -> Result<(), String>;
    /// Most recent OTP `created_at_unix` for (user, otp_type), used for send /
    /// resend cooldown enforcement. Default `None` (cooldown not enforced) —
    /// overridden by the Postgres store.
    async fn latest_otp_created_at(
        &self,
        _user_id: &str,
        _otp_type: i32,
    ) -> Result<Option<u64>, String> {
        Ok(None)
    }
    /// Atomically flip a PENDING OTP to USED, returning true only for the caller
    /// that won the race. Guarantees single-use even under concurrent submissions
    /// of the same otp_id. Default fails closed (no consume).
    async fn consume_otp_pending(&self, _otp_id: &str, _now_unix: u64) -> Result<bool, String> {
        Ok(false)
    }
    /// Replace the user's MFA recovery codes with `code_hashes` (prior codes are
    /// discarded). Default fails closed; overridden by the Postgres store.
    async fn replace_recovery_codes(
        &self,
        _user_id: &str,
        _tenant_id: &str,
        _code_hashes: &[String],
    ) -> Result<(), String> {
        Err("recovery codes require the Postgres user store".to_string())
    }
    /// Atomically consume one unused recovery code matching `code_hash` for the
    /// user, returning true on a successful single-use match. Default: no match.
    async fn consume_recovery_code(
        &self,
        _user_id: &str,
        _code_hash: &str,
        _now_unix: u64,
    ) -> Result<bool, String> {
        Ok(false)
    }
    /// Upsert the per-tenant MFA-enforcement policy. Default fails closed.
    async fn put_mfa_policy(&self, _tenant_id: &str, _require_mfa: bool) -> Result<(), String> {
        Err("MFA policy requires the Postgres user store".to_string())
    }
    /// Whether the tenant requires MFA for password login. Default: false (no
    /// policy store) — enforcement is additive and must never fail open here.
    async fn tenant_requires_mfa(&self, _tenant_id: &str) -> Result<bool, String> {
        Ok(false)
    }
    /// Set the user's phone number (clearing any prior verification). Default
    /// fails closed; overridden by the Postgres store.
    async fn set_user_phone(&self, _user_id: &str, _phone: &str) -> Result<(), String> {
        Err("phone verification requires the Postgres user store".to_string())
    }
    /// Mark the user's phone number verified. Default fails closed.
    async fn mark_phone_verified(&self, _user_id: &str, _now_unix: u64) -> Result<(), String> {
        Err("phone verification requires the Postgres user store".to_string())
    }
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
        _account_kind: AccountKind,
        _status: AccountStatus,
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

fn native_compile_context() -> CompileContext<'static> {
    CompileContext::new(crate::runtime::native_catalog::native_manifest())
}

const SESSION_MSG: &str = "udb.core.authn.entity.v1.Session";
const API_KEY_MSG: &str = "udb.core.apikey.entity.v1.ApiKey";
const USER_MSG: &str = "udb.core.authn.entity.v1.User";
const OTP_MSG: &str = "udb.core.authn.entity.v1.OTP";
const RECOVERY_CODE_MSG: &str = "udb.core.authn.entity.v1.RecoveryCode";
const MFA_POLICY_MSG: &str = "udb.core.authn.entity.v1.MfaPolicy";

fn logical_ts(unix: u64) -> Result<LogicalValue, String> {
    chrono::DateTime::from_timestamp(unix as i64, 0)
        .map(LogicalValue::Timestamp)
        .ok_or_else(|| format!("unix timestamp {unix} is out of range"))
}

fn logical_ts_or_null(unix: u64) -> Result<LogicalValue, String> {
    if unix == 0 {
        Ok(LogicalValue::Null)
    } else {
        logical_ts(unix)
    }
}

fn uuid_or_null(value: &str) -> LogicalValue {
    if value.trim().is_empty() {
        LogicalValue::Null
    } else {
        LogicalValue::String(value.trim().to_string())
    }
}

fn eq(field: &str, value: LogicalValue) -> LogicalFilter {
    LogicalFilter::Comparison {
        field: field.to_string(),
        op: ComparisonOp::Eq,
        value,
    }
}

async fn execute_typed_update_on<'c, E>(executor: E, op: LogicalUpdate) -> Result<u64, String>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let ctx = native_compile_context();
    let compiled = crate::runtime::service::handlers_data::compile_logical_update_dispatch(
        &BackendKind::Postgres,
        &op,
        &ctx,
    )
    .map_err(|err| format!("compile typed authn update failed: {err}"))?;
    let spec: serde_json::Value = serde_json::from_str(&compiled.spec_json)
        .map_err(|err| format!("typed authn update JSON failed: {err}"))?;
    let sql = spec
        .get("sql")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "typed authn update did not compile to SQL".to_string())?;
    crate::runtime::core::validate_pg_mutation_sql(sql).map_err(|err| err.to_string())?;
    let mut params = crate::runtime::core::dispatch_params(&spec).map_err(|err| err.to_string())?;
    strip_nul_text_params(&mut params, "authn update");
    let param_types =
        crate::runtime::core::dispatch_param_types(&spec).map_err(|err| err.to_string())?;
    let result = crate::runtime::core::bind_typed_generic_pg_params(
        sqlx::query(sql),
        &params,
        param_types.as_deref(),
    )
    .map_err(|err| err.to_string())?
    .execute(executor)
    .await
    .map_err(|err| {
        crate::runtime::executor_utils::sqlx_error_to_tagged_string(
            "typed authn update failed",
            &err,
        )
    })?;
    Ok(result.rows_affected())
}

async fn execute_typed_write_on<'c, E>(executor: E, op: LogicalWrite) -> Result<u64, String>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let ctx = native_compile_context();
    let compiled = crate::runtime::service::handlers_data::compile_logical_write_dispatch(
        &BackendKind::Postgres,
        &op,
        &ctx,
    )
    .map_err(|err| format!("compile typed authn write failed: {err}"))?;
    let spec: serde_json::Value = serde_json::from_str(&compiled.spec_json)
        .map_err(|err| format!("typed authn write JSON failed: {err}"))?;
    let sql = spec
        .get("sql")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "typed authn write did not compile to SQL".to_string())?;
    crate::runtime::core::validate_pg_mutation_sql(sql).map_err(|err| err.to_string())?;
    let mut params = crate::runtime::core::dispatch_params(&spec).map_err(|err| err.to_string())?;
    strip_nul_text_params(&mut params, "authn write");
    let param_types =
        crate::runtime::core::dispatch_param_types(&spec).map_err(|err| err.to_string())?;
    let bound = crate::runtime::core::bind_typed_generic_pg_params(
        sqlx::query(sql),
        &params,
        param_types.as_deref(),
    )
    .map_err(|err| err.to_string())?;
    match bound.execute(executor).await {
        Ok(result) => Ok(result.rows_affected()),
        Err(err) => {
            let msg = err.to_string();
            if msg.contains("0x00") || msg.contains("invalid byte sequence") {
                // B14 DIAG: locate the NUL definitively — is it in the SQL text or a
                // specific param (and of what param_type)?
                tracing::error!(
                    sql_has_nul = sql.contains('\u{0}'),
                    "B14 DIAG typed authn write 0x00; dumping params"
                );
                for (i, p) in params.iter().enumerate() {
                    let txt = p.to_string();
                    if txt.contains('\u{0}') {
                        let pt = param_types
                            .as_ref()
                            .and_then(|t| t.get(i))
                            .cloned()
                            .unwrap_or_default();
                        tracing::error!(idx = i, ptype = %pt, "B14 DIAG param CONTAINS NUL");
                    }
                }
            }
            Err(crate::runtime::executor_utils::sqlx_error_to_tagged_string(
                "typed authn write failed",
                &err,
            ))
        }
    }
}

/// B14: A NUL byte (`0x00`) cannot exist in a Postgres `text`/`varchar`/`json`
/// value — the server rejects the whole statement with
/// `invalid byte sequence for encoding "UTF8": 0x00`. A typed authn write value
/// that carries one is upstream corruption (a binary/random value bound as text
/// instead of being hex/base64-encoded), and it can never be persisted as-is, so
/// failing the Login/Refresh hard helps nobody. Strip the NUL(s) so the
/// credential/session write succeeds, and warn so the un-encoded source field can
/// be found and fixed at the root. Operates on the JSON params just before binding,
/// the single chokepoint shared by every typed authn write/update.
fn strip_nul_text_params(params: &mut [serde_json::Value], op: &str) {
    let mut stripped = 0usize;
    for param in params.iter_mut() {
        stripped += strip_nul_in_json(param);
    }
    if stripped > 0 {
        tracing::warn!(
            op = %op,
            stripped,
            "typed {op}: stripped NUL byte(s) from text param(s) (B14) — an upstream \
             authn field is binding un-encoded binary as text; encode it (hex/base64) \
             or store it as bytea at the source"
        );
    }
}

/// Recursively strip NUL (`\u{0}`) bytes from every string anywhere in a JSON
/// param — top-level strings, array elements, and object values alike — and
/// return how many strings were rewritten. A NUL can never be stored in a
/// Postgres `text`/`varchar`/`text[]` value, so this is always safe; it just
/// turns a hard write failure into a successful one. (See [`strip_nul_text_params`].)
fn strip_nul_in_json(value: &mut serde_json::Value) -> usize {
    match value {
        serde_json::Value::String(text) => {
            if text.contains('\u{0}') {
                *text = text.replace('\u{0}', "");
                1
            } else {
                0
            }
        }
        serde_json::Value::Array(items) => items.iter_mut().map(strip_nul_in_json).sum(),
        serde_json::Value::Object(map) => map.values_mut().map(strip_nul_in_json).sum(),
        _ => 0,
    }
}

async fn execute_typed_write(pool: &PgPool, op: LogicalWrite) -> Result<u64, String> {
    execute_typed_write_on(pool, op).await
}

async fn execute_typed_delete_on<'c, E>(executor: E, op: LogicalDelete) -> Result<u64, String>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let ctx = native_compile_context();
    let compiled = crate::runtime::service::handlers_data::compile_logical_delete_dispatch(
        &BackendKind::Postgres,
        &op,
        &ctx,
    )
    .map_err(|err| format!("compile typed authn delete failed: {err}"))?;
    let spec: serde_json::Value = serde_json::from_str(&compiled.spec_json)
        .map_err(|err| format!("typed authn delete JSON failed: {err}"))?;
    let sql = spec
        .get("sql")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "typed authn delete did not compile to SQL".to_string())?;
    crate::runtime::core::validate_pg_mutation_sql(sql).map_err(|err| err.to_string())?;
    let params = crate::runtime::core::dispatch_params(&spec).map_err(|err| err.to_string())?;
    let param_types =
        crate::runtime::core::dispatch_param_types(&spec).map_err(|err| err.to_string())?;
    let result = crate::runtime::core::bind_typed_generic_pg_params(
        sqlx::query(sql),
        &params,
        param_types.as_deref(),
    )
    .map_err(|err| err.to_string())?
    .execute(executor)
    .await
    .map_err(|err| format!("typed authn delete failed: {err}"))?;
    Ok(result.rows_affected())
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

impl PostgresSessionStore {
    /// `revoke_all_for_principal` over any executor — `&self.pool` for a standalone
    /// revoke, or a `&mut PgConnection` so it commits in the caller's transaction.
    pub(crate) async fn revoke_all_for_principal_on<'c, E>(
        &self,
        executor: E,
        principal_id: &str,
        now_unix: u64,
    ) -> Result<usize, String>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        let mut assignments = std::collections::BTreeMap::new();
        assignments.insert(
            "is_active".to_string(),
            LogicalAssignment::Set {
                value: LogicalValue::Bool(false),
            },
        );
        assignments.insert(
            "last_active_at".to_string(),
            LogicalAssignment::Set {
                value: logical_ts(now_unix)?,
            },
        );
        assignments.insert(
            "revoke_reason".to_string(),
            LogicalAssignment::Set {
                value: LogicalValue::String("principal_revoke".to_string()),
            },
        );
        execute_typed_update_on(
            executor,
            LogicalUpdate {
                message_type: SESSION_MSG.to_string(),
                filter: LogicalFilter::And(vec![
                    eq(
                        "principal_id",
                        LogicalValue::String(principal_id.to_string()),
                    ),
                    eq("is_active", LogicalValue::Bool(true)),
                ]),
                assignments,
                return_fields: Vec::new(),
                require_affected: false,
            },
        )
        .await
        .map(|rows| rows as usize)
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
        let mut assignments = std::collections::BTreeMap::new();
        assignments.insert(
            "is_active".to_string(),
            LogicalAssignment::Set {
                value: LogicalValue::Bool(false),
            },
        );
        assignments.insert(
            "last_active_at".to_string(),
            LogicalAssignment::Set {
                value: logical_ts(now_unix)?,
            },
        );
        assignments.insert(
            "revoke_reason".to_string(),
            LogicalAssignment::Set {
                value: LogicalValue::String("revoked".to_string()),
            },
        );
        let affected = execute_typed_update_on(
            &self.pool,
            LogicalUpdate {
                message_type: SESSION_MSG.to_string(),
                filter: LogicalFilter::And(vec![
                    eq(
                        "session_token_lookup",
                        LogicalValue::String(session_id_hash.to_string()),
                    ),
                    eq("is_active", LogicalValue::Bool(true)),
                ]),
                assignments,
                return_fields: Vec::new(),
                require_affected: false,
            },
        )
        .await?;
        Ok(affected > 0)
    }

    async fn touch_last_active(&self, session_id_hash: &str, now_unix: u64) -> Result<(), String> {
        let mut assignments = std::collections::BTreeMap::new();
        assignments.insert(
            "last_active_at".to_string(),
            LogicalAssignment::Set {
                value: logical_ts(now_unix)?,
            },
        );
        execute_typed_update_on(
            &self.pool,
            LogicalUpdate {
                message_type: SESSION_MSG.to_string(),
                filter: LogicalFilter::And(vec![
                    eq(
                        "session_token_lookup",
                        LogicalValue::String(session_id_hash.to_string()),
                    ),
                    eq("is_active", LogicalValue::Bool(true)),
                ]),
                assignments,
                return_fields: Vec::new(),
                require_affected: false,
            },
        )
        .await?;
        Ok(())
    }

    async fn revoke_all_for_principal(
        &self,
        principal_id: &str,
        now_unix: u64,
    ) -> Result<usize, String> {
        self.revoke_all_for_principal_on(&self.pool, principal_id, now_unix)
            .await
    }

    async fn revoke_all_for_principal_in_tx(
        &self,
        conn: &mut sqlx::PgConnection,
        principal_id: &str,
        now_unix: u64,
    ) -> Result<usize, String> {
        self.revoke_all_for_principal_on(&mut *conn, principal_id, now_unix)
            .await
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

fn api_key_write_op(record: &ApiKeyRecord, status: ApiKeyStatus) -> Result<LogicalWrite, String> {
    let metadata_json = serde_json::json!({
        "service_identity": record.service_identity,
        "grant_revision": record.grant_revision,
    });
    let mut row = LogicalRecord::new();
    row.insert(
        "key_prefix".to_string(),
        LogicalValue::String(record.key_prefix.clone()),
    );
    row.insert(
        "key_hash".to_string(),
        LogicalValue::String(record.key_hash.clone()),
    );
    row.insert(
        "name".to_string(),
        LogicalValue::String(if record.name.trim().is_empty() {
            record.key_prefix.clone()
        } else {
            record.name.clone()
        }),
    );
    row.insert(
        "description".to_string(),
        LogicalValue::String(record.description.clone()),
    );
    row.insert(
        "owner_type".to_string(),
        LogicalValue::String("SERVICE_ACCOUNT".to_string()),
    );
    row.insert(
        "owner_id".to_string(),
        LogicalValue::String(record.principal_id.clone()),
    );
    row.insert(
        "scopes_json".to_string(),
        LogicalValue::Json(
            serde_json::from_str(&string_list_to_json(&record.scopes))
                .map_err(|err| format!("api key scopes JSON failed: {err}"))?,
        ),
    );
    row.insert(
        "status".to_string(),
        LogicalValue::String(api_key_status_to_db(status).to_string()),
    );
    row.insert(
        "last_used_at".to_string(),
        logical_ts_or_null(record.last_used_at_unix)?,
    );
    row.insert(
        "expires_at".to_string(),
        logical_ts_or_null(record.expires_at_unix)?,
    );
    row.insert(
        "created_by".to_string(),
        LogicalValue::String(record.principal_id.clone()),
    );
    row.insert(
        "tenant_id".to_string(),
        LogicalValue::String(record.tenant_id.clone()),
    );
    row.insert(
        "project_id".to_string(),
        LogicalValue::String(record.project_id.clone()),
    );
    row.insert(
        "metadata_json".to_string(),
        LogicalValue::Json(metadata_json),
    );
    row.insert(
        "deleted_at".to_string(),
        logical_ts_or_null(record.revoked_at_unix)?,
    );
    row.insert(
        "deleted_by".to_string(),
        LogicalValue::String(if record.revoked_at_unix > 0 {
            record.principal_id.clone()
        } else {
            String::new()
        }),
    );
    Ok(LogicalWrite {
        message_type: API_KEY_MSG.to_string(),
        records: vec![row],
        conflict: ConflictStrategy::update_on(
            vec![
                "key_prefix".to_string(),
                "name".to_string(),
                "description".to_string(),
                "owner_id".to_string(),
                "scopes_json".to_string(),
                "status".to_string(),
                "last_used_at".to_string(),
                "expires_at".to_string(),
                "tenant_id".to_string(),
                "project_id".to_string(),
                "metadata_json".to_string(),
                "deleted_at".to_string(),
                "deleted_by".to_string(),
            ],
            vec!["key_hash".to_string()],
        ),
        return_fields: Vec::new(),
    })
}

impl PostgresApiKeyStore {
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
            ApiKeyStatus::Revoked
        } else {
            ApiKeyStatus::Active
        };
        execute_typed_write(&self.pool, api_key_write_op(&record, status)?).await?;
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
            ApiKeyStatus::Revoked
        } else {
            ApiKeyStatus::Active
        };
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|err| format!("rotate api key begin failed: {err}"))?;
        execute_typed_write_on(&mut *tx, api_key_write_op(&new_record, status)?)
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
        let name = m.text_or_empty("name");
        let description = m.text_or_empty("description");
        let owner_id = m.select_as("owner_id", "principal_id");
        let service_identity =
            m.json_get_as("metadata_json", "service_identity", "service_identity");
        let grant_revision = m.json_get_as("metadata_json", "grant_revision", "grant_revision");
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
            "SELECT {key_prefix_col} AS key_prefix, {key_hash}, {name}, {description}, {owner_id}, {service_identity}, {grant_revision}, {tenant_id}, {project_id}, {scopes}, \
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
        let name = m.text_or_empty("name");
        let description = m.text_or_empty("description");
        let owner_id = m.select_as("owner_id", "principal_id");
        let owner_id_col = m.q("owner_id");
        let service_identity =
            m.json_get_as("metadata_json", "service_identity", "service_identity");
        let grant_revision = m.json_get_as("metadata_json", "grant_revision", "grant_revision");
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
            "SELECT {key_prefix_col}, {key_hash}, {name}, {description}, {owner_id}, {service_identity}, {grant_revision}, {tenant_id}, {project_id}, {scopes}, \
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
        let name = m.text_or_empty("name");
        let description = m.text_or_empty("description");
        let owner_id = m.select_as("owner_id", "principal_id");
        let owner_id_col = m.q("owner_id");
        let service_identity =
            m.json_get_as("metadata_json", "service_identity", "service_identity");
        let grant_revision = m.json_get_as("metadata_json", "grant_revision", "grant_revision");
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
            "SELECT {key_prefix_col}, {key_hash}, {name}, {description}, {owner_id}, {service_identity}, {grant_revision}, {tenant_id}, {project_id}, {scopes}, \
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
        status: ApiKeyStatus,
        now_unix: u64,
        limit: usize,
        offset: usize,
    ) -> Result<(Vec<ApiKeyRecord>, usize), String> {
        let rel = self.relation();
        let m = &self.model;
        let key_prefix_col = m.select("key_prefix");
        let key_hash = m.select("key_hash");
        let name = m.text_or_empty("name");
        let description = m.text_or_empty("description");
        let owner_id = m.select_as("owner_id", "principal_id");
        let owner_id_col = m.q("owner_id");
        let service_identity =
            m.json_get_as("metadata_json", "service_identity", "service_identity");
        let grant_revision = m.json_get_as("metadata_json", "grant_revision", "grant_revision");
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
            ApiKeyStatus::Unspecified => String::new(),
            ApiKeyStatus::Active => format!(
                "AND {deleted_at} IS NULL AND {status_col} = 'ACTIVE' AND ({expires_at} IS NULL OR {expires_at} > to_timestamp($2::DOUBLE PRECISION))"
            ),
            ApiKeyStatus::Revoked => {
                format!("AND ({deleted_at} IS NOT NULL OR {status_col} = 'REVOKED')")
            }
            ApiKeyStatus::Expired => format!(
                "AND {deleted_at} IS NULL AND ({status_col} = 'EXPIRED' OR ({status_col} = 'ACTIVE' AND {expires_at} IS NOT NULL AND {expires_at} <= to_timestamp($2::DOUBLE PRECISION)))"
            ),
        };
        let count_sql = format!(
            "SELECT COUNT(*)::BIGINT AS total_count FROM {rel} WHERE ({owner_id_col} = $1 OR {service_identity_expr} = $1) {status_clause}"
        );
        let mut count_query = sqlx::query(&count_sql).bind(principal_id);
        if matches!(status, ApiKeyStatus::Active | ApiKeyStatus::Expired) {
            count_query = count_query.bind(now_unix as i64);
        }
        let total = count_query
            .fetch_one(&self.pool)
            .await
            .map_err(|err| format!("count api keys failed: {err}"))?
            .try_get::<i64, _>("total_count")
            .map_err(|err| format!("decode api key count failed: {err}"))?
            .max(0) as usize;
        let (limit_param, offset_param) =
            if matches!(status, ApiKeyStatus::Active | ApiKeyStatus::Expired) {
                ("$3", "$4")
            } else {
                ("$2", "$3")
            };
        let sql = format!(
            "SELECT {key_prefix_col}, {key_hash}, {name}, {description}, {owner_id}, {service_identity}, {grant_revision}, {tenant_id}, {project_id}, {scopes}, \
                    {created_at_unix}, {last_used_at_unix}, {expires_at_unix}, {revoked_at_unix} \
             FROM {rel} WHERE ({owner_id_col} = $1 OR {service_identity_expr} = $1) {status_clause} ORDER BY {created_at} DESC LIMIT {limit_param} OFFSET {offset_param}"
        );
        let mut query = sqlx::query(&sql).bind(principal_id);
        if matches!(status, ApiKeyStatus::Active | ApiKeyStatus::Expired) {
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
        let mut assignments = std::collections::BTreeMap::new();
        assignments.insert(
            "last_used_at".to_string(),
            LogicalAssignment::Set {
                value: logical_ts(now_unix)?,
            },
        );
        execute_typed_update_on(
            &self.pool,
            LogicalUpdate {
                message_type: API_KEY_MSG.to_string(),
                filter: LogicalFilter::And(vec![
                    eq("key_prefix", LogicalValue::String(key_prefix.to_string())),
                    LogicalFilter::IsNull("deleted_at".to_string()),
                ]),
                assignments,
                return_fields: Vec::new(),
                require_affected: false,
            },
        )
        .await?;
        Ok(())
    }
}

pub struct PostgresUserStore {
    pool: PgPool,
    users_model: NativeModel,
    otps_model: NativeModel,
    mfa_policies_model: NativeModel,
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
                    "phone",
                    "phone_verified_at",
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
            mfa_policies_model: native_model(
                "udb.core.authn.entity.v1.MfaPolicy",
                &[
                    "policy_id",
                    "tenant_id",
                    "require_mfa",
                    "created_at",
                    "updated_at",
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
            m.text_or_empty("tenant_id"),
            m.timestamp_unix_as("expires_at", "expires_at_unix"),
            m.timestamp_unix_as("used_at", "used_at_unix"),
            m.timestamp_unix_as("created_at", "created_at_unix"),
        ]
        .join(", ")
    }
}

impl PostgresUserStore {
    /// `put_user` over any executor — `&self.pool` for a standalone write, or a
    /// `&mut PgConnection` so the upsert commits inside the caller's transaction
    /// (used by `put_user_in_tx` for enterprise auth-mutation atomicity).
    pub(crate) async fn put_user_on<'c, E>(
        &self,
        executor: E,
        record: UserRecord,
    ) -> Result<(), String>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        let mut row = LogicalRecord::new();
        row.insert("user_id".to_string(), LogicalValue::String(record.user_id));
        row.insert(
            "username".to_string(),
            LogicalValue::String(record.username),
        );
        row.insert("email".to_string(), LogicalValue::String(record.email));
        row.insert(
            "password_hash".to_string(),
            LogicalValue::String(record.password_hash),
        );
        row.insert(
            "account_kind".to_string(),
            LogicalValue::String(record.account_kind.to_db().to_string()),
        );
        row.insert(
            "status".to_string(),
            LogicalValue::String(record.status.to_db().to_string()),
        );
        row.insert(
            "tenant_id".to_string(),
            LogicalValue::String(record.tenant_id),
        );
        row.insert(
            "full_name".to_string(),
            LogicalValue::String(record.full_name),
        );
        row.insert(
            "totp_secret_enc".to_string(),
            LogicalValue::String(record.totp_secret_hash),
        );
        row.insert(
            "mfa_enabled".to_string(),
            LogicalValue::Bool(record.mfa_enabled),
        );
        row.insert(
            "failed_login_count".to_string(),
            LogicalValue::Int(record.failed_login_count.into()),
        );
        row.insert(
            "locked_until".to_string(),
            logical_ts_or_null(record.locked_until_unix)?,
        );
        row.insert(
            "email_verified_at".to_string(),
            logical_ts_or_null(record.email_verified_at_unix)?,
        );
        row.insert(
            "last_login_at".to_string(),
            logical_ts_or_null(record.last_login_at_unix)?,
        );
        row.insert("created_by".to_string(), uuid_or_null(&record.created_by));
        row.insert(
            "created_at".to_string(),
            logical_ts(record.created_at_unix)?,
        );
        row.insert(
            "updated_at".to_string(),
            logical_ts(record.updated_at_unix)?,
        );
        row.insert(
            "deleted_at".to_string(),
            logical_ts_or_null(record.deleted_at_unix)?,
        );
        row.insert("deleted_by".to_string(), uuid_or_null(&record.deleted_by));
        row.insert(
            "project_id".to_string(),
            LogicalValue::String(record.project_id),
        );
        row.insert(
            "external_provider_id".to_string(),
            LogicalValue::String(record.external_provider_id),
        );
        row.insert(
            "external_subject".to_string(),
            LogicalValue::String(record.external_subject),
        );
        row.insert(
            "profile_attributes_json".to_string(),
            LogicalValue::Json(
                serde_json::from_str(&json_object_or_empty(&record.profile_attributes_json))
                    .map_err(|err| format!("profile attributes JSON failed: {err}"))?,
            ),
        );
        execute_typed_write_on(
            executor,
            LogicalWrite {
                message_type: USER_MSG.to_string(),
                records: vec![row],
                conflict: ConflictStrategy::update(vec![
                    "username".to_string(),
                    "email".to_string(),
                    "password_hash".to_string(),
                    "account_kind".to_string(),
                    "status".to_string(),
                    "tenant_id".to_string(),
                    "full_name".to_string(),
                    "totp_secret_enc".to_string(),
                    "mfa_enabled".to_string(),
                    "failed_login_count".to_string(),
                    "locked_until".to_string(),
                    "email_verified_at".to_string(),
                    "last_login_at".to_string(),
                    "updated_at".to_string(),
                    "deleted_at".to_string(),
                    "deleted_by".to_string(),
                    "project_id".to_string(),
                    "external_provider_id".to_string(),
                    "external_subject".to_string(),
                    "profile_attributes_json".to_string(),
                ]),
                return_fields: Vec::new(),
            },
        )
        .await?;
        Ok(())
    }

    /// `put_otp` over any executor — `&self.pool` for a standalone write, or a
    /// `&mut PgConnection` so the OTP upsert commits inside the caller's
    /// transaction (used by `put_otp_in_tx` for `create_user` atomicity).
    pub(crate) async fn put_otp_on<'c, E>(
        &self,
        executor: E,
        record: OtpRecord,
    ) -> Result<(), String>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        let mut row = LogicalRecord::new();
        row.insert("otp_id".to_string(), LogicalValue::String(record.otp_id));
        row.insert("user_id".to_string(), LogicalValue::String(record.user_id));
        row.insert(
            "otp_type".to_string(),
            LogicalValue::String(otp_type_to_db(record.otp_type).to_string()),
        );
        row.insert(
            "code_hash".to_string(),
            LogicalValue::String(record.code_hash),
        );
        row.insert(
            "delivery_channel".to_string(),
            LogicalValue::String(record.delivery_channel),
        );
        row.insert(
            "delivery_address".to_string(),
            LogicalValue::String(record.delivery_address),
        );
        row.insert(
            "status".to_string(),
            LogicalValue::String(otp_status_to_db(record.status).to_string()),
        );
        row.insert(
            "attempt_count".to_string(),
            LogicalValue::Int(record.attempt_count.into()),
        );
        row.insert(
            "superseded_by_id".to_string(),
            uuid_or_null(&record.superseded_by_id),
        );
        row.insert(
            "expires_at".to_string(),
            logical_ts(record.expires_at_unix)?,
        );
        row.insert(
            "used_at".to_string(),
            logical_ts_or_null(record.used_at_unix)?,
        );
        row.insert(
            "created_at".to_string(),
            logical_ts(record.created_at_unix)?,
        );
        row.insert(
            "correlation_id".to_string(),
            LogicalValue::String(record.correlation_id),
        );
        row.insert(
            "tenant_id".to_string(),
            if record.tenant_id.trim().is_empty() {
                LogicalValue::Null
            } else {
                LogicalValue::String(record.tenant_id)
            },
        );
        execute_typed_write_on(
            executor,
            LogicalWrite {
                message_type: OTP_MSG.to_string(),
                records: vec![row],
                conflict: ConflictStrategy::update(vec![
                    "status".to_string(),
                    "attempt_count".to_string(),
                    "superseded_by_id".to_string(),
                    "used_at".to_string(),
                ]),
                return_fields: Vec::new(),
            },
        )
        .await?;
        Ok(())
    }
}

#[async_trait]
impl UserStore for PostgresUserStore {
    async fn put_user(&self, record: UserRecord) -> Result<(), String> {
        self.put_user_on(&self.pool, record).await
    }

    fn backing_pool(&self) -> Option<PgPool> {
        Some(self.pool.clone())
    }

    /// Atomic variant: upsert the user on the caller's transaction connection so
    /// it commits with the caller's other writes (e.g. its audit/outbox event).
    async fn put_user_in_tx(
        &self,
        conn: &mut sqlx::PgConnection,
        record: UserRecord,
    ) -> Result<(), String> {
        self.put_user_on(&mut *conn, record).await
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

    async fn get_user_by_id_in_tenant(
        &self,
        user_id: &str,
        tenant_id: &str,
    ) -> Result<Option<UserRecord>, String> {
        self.get_user_by_in_tenant("user_id", user_id, tenant_id)
            .await
    }

    async fn get_user_by_username_in_tenant(
        &self,
        username: &str,
        tenant_id: &str,
    ) -> Result<Option<UserRecord>, String> {
        self.get_user_by_in_tenant("username", username, tenant_id)
            .await
    }

    async fn get_user_by_email_in_tenant(
        &self,
        email: &str,
        tenant_id: &str,
    ) -> Result<Option<UserRecord>, String> {
        self.get_user_by_in_tenant("email", email, tenant_id).await
    }

    async fn list_users(
        &self,
        tenant_id: &str,
        account_kind: AccountKind,
        status: AccountStatus,
    ) -> Result<Vec<UserRecord>, String> {
        let rel = self.users_relation();
        let projection = self.user_select_projection();
        let tenant_id_col = self.users_model.q("tenant_id");
        let account_kind_col = self.users_model.q("account_kind");
        let status_col = self.users_model.q("status");
        let created_at = self.users_model.q("created_at");
        let deleted_at = self.users_model.q("deleted_at");
        let account_kind_filter = account_kind.to_db();
        let status_filter = status.to_db();
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
        account_kind: AccountKind,
        status: AccountStatus,
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
        let account_kind_filter = account_kind.to_db();
        let status_filter = status.to_db();
        let where_clause = format!(
            "{deleted_at} IS NULL AND ($1 = '' OR {tenant_id_col} = $1) AND ($2 = 'UNSPECIFIED' OR {account_kind_col} = $2) AND ($3 = 'UNSPECIFIED' OR {status_col} = $3)"
        );
        let total = sqlx::query(&format!(
            "SELECT COUNT(*)::BIGINT AS total_count FROM {rel} WHERE {where_clause}"
        ))
        .bind(tenant_id)
        .bind(account_kind_filter)
        .bind(status_filter)
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
        let mut assignments = std::collections::BTreeMap::new();
        assignments.insert(
            "deleted_at".to_string(),
            LogicalAssignment::Set {
                value: logical_ts(now_unix)?,
            },
        );
        assignments.insert(
            "deleted_by".to_string(),
            LogicalAssignment::Set {
                value: uuid_or_null(deleted_by),
            },
        );
        assignments.insert(
            "updated_at".to_string(),
            LogicalAssignment::Set {
                value: logical_ts(now_unix)?,
            },
        );
        let affected = execute_typed_update_on(
            &self.pool,
            LogicalUpdate {
                message_type: USER_MSG.to_string(),
                filter: LogicalFilter::And(vec![
                    eq("user_id", LogicalValue::String(user_id.to_string())),
                    LogicalFilter::IsNull("deleted_at".to_string()),
                ]),
                assignments,
                return_fields: Vec::new(),
                require_affected: false,
            },
        )
        .await?;
        Ok(affected > 0)
    }

    async fn put_otp(&self, record: OtpRecord) -> Result<(), String> {
        self.put_otp_on(&self.pool, record).await
    }

    /// Atomic variant: upsert the OTP on the caller's transaction connection so
    /// it commits with the caller's other writes (e.g. `create_user`'s user
    /// upsert + audit/outbox event).
    async fn put_otp_in_tx(
        &self,
        conn: &mut sqlx::PgConnection,
        record: OtpRecord,
    ) -> Result<(), String> {
        self.put_otp_on(&mut *conn, record).await
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

    async fn latest_otp_created_at(
        &self,
        user_id: &str,
        otp_type: i32,
    ) -> Result<Option<u64>, String> {
        let rel = self.otps_relation();
        let user_col = self.otps_model.q("user_id");
        let type_col = self.otps_model.q("otp_type");
        let created_col = self.otps_model.q("created_at");
        // `created_at` is a TIMESTAMP and `otp_type` is stored as its short token
        // (see `otp_type_to_db`), so match on the token and project the max as epoch.
        let epoch: Option<i64> = sqlx::query_scalar(&format!(
            "SELECT EXTRACT(EPOCH FROM MAX({created_col}))::BIGINT \
             FROM {rel} WHERE {user_col} = $1::UUID AND {type_col} = $2"
        ))
        .bind(user_id)
        .bind(otp_type_to_db(otp_type))
        .fetch_one(&self.pool)
        .await
        .map_err(|err| format!("latest otp lookup failed: {err}"))?;
        Ok(epoch.map(|v| v.max(0) as u64))
    }

    async fn consume_otp_pending(&self, otp_id: &str, now_unix: u64) -> Result<bool, String> {
        let mut assignments = std::collections::BTreeMap::new();
        assignments.insert(
            "status".to_string(),
            LogicalAssignment::Set {
                value: LogicalValue::String(otp_status_to_db(OtpStatus::Used.as_i32()).to_string()),
            },
        );
        assignments.insert(
            "used_at".to_string(),
            LogicalAssignment::Set {
                value: logical_ts(now_unix)?,
            },
        );
        let affected = execute_typed_update_on(
            &self.pool,
            LogicalUpdate {
                message_type: OTP_MSG.to_string(),
                filter: LogicalFilter::And(vec![
                    eq("otp_id", LogicalValue::String(otp_id.to_string())),
                    eq(
                        "status",
                        LogicalValue::String(
                            otp_status_to_db(OtpStatus::Pending.as_i32()).to_string(),
                        ),
                    ),
                ]),
                assignments,
                return_fields: Vec::new(),
                require_affected: false,
            },
        )
        .await?;
        Ok(affected > 0)
    }

    async fn replace_recovery_codes(
        &self,
        user_id: &str,
        tenant_id: &str,
        code_hashes: &[String],
    ) -> Result<(), String> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|err| format!("recovery codes tx begin failed: {err}"))?;
        execute_typed_delete_on(
            &mut *tx,
            LogicalDelete {
                message_type: RECOVERY_CODE_MSG.to_string(),
                filter: eq("user_id", LogicalValue::String(user_id.to_string())),
                return_fields: Vec::new(),
            },
        )
        .await
        .map_err(|err| format!("recovery codes clear failed: {err}"))?;
        for hash in code_hashes {
            let mut row = LogicalRecord::new();
            row.insert(
                "user_id".to_string(),
                LogicalValue::String(user_id.to_string()),
            );
            row.insert("code_hash".to_string(), LogicalValue::String(hash.clone()));
            row.insert(
                "tenant_id".to_string(),
                if tenant_id.trim().is_empty() {
                    LogicalValue::Null
                } else {
                    LogicalValue::String(tenant_id.to_string())
                },
            );
            execute_typed_write_on(
                &mut *tx,
                LogicalWrite {
                    message_type: RECOVERY_CODE_MSG.to_string(),
                    records: vec![row],
                    conflict: ConflictStrategy::Error,
                    return_fields: Vec::new(),
                },
            )
            .await
            .map_err(|err| format!("recovery code insert failed: {err}"))?;
        }
        tx.commit()
            .await
            .map_err(|err| format!("recovery codes tx commit failed: {err}"))
    }

    async fn consume_recovery_code(
        &self,
        user_id: &str,
        code_hash: &str,
        now_unix: u64,
    ) -> Result<bool, String> {
        let mut assignments = std::collections::BTreeMap::new();
        assignments.insert(
            "used_at".to_string(),
            LogicalAssignment::Set {
                value: logical_ts(now_unix)?,
            },
        );
        let affected = execute_typed_update_on(
            &self.pool,
            LogicalUpdate {
                message_type: RECOVERY_CODE_MSG.to_string(),
                filter: LogicalFilter::And(vec![
                    eq("user_id", LogicalValue::String(user_id.to_string())),
                    eq("code_hash", LogicalValue::String(code_hash.to_string())),
                    LogicalFilter::IsNull("used_at".to_string()),
                ]),
                assignments,
                return_fields: Vec::new(),
                require_affected: false,
            },
        )
        .await?;
        Ok(affected > 0)
    }

    async fn put_mfa_policy(&self, tenant_id: &str, require_mfa: bool) -> Result<(), String> {
        let mut row = LogicalRecord::new();
        row.insert(
            "tenant_id".to_string(),
            LogicalValue::String(tenant_id.to_string()),
        );
        row.insert("require_mfa".to_string(), LogicalValue::Bool(require_mfa));
        row.insert(
            "updated_at".to_string(),
            LogicalValue::Timestamp(chrono::Utc::now()),
        );
        execute_typed_write(
            &self.pool,
            LogicalWrite {
                message_type: MFA_POLICY_MSG.to_string(),
                records: vec![row],
                conflict: ConflictStrategy::update_on(
                    vec!["require_mfa".to_string(), "updated_at".to_string()],
                    vec!["tenant_id".to_string()],
                ),
                return_fields: Vec::new(),
            },
        )
        .await?;
        Ok(())
    }

    async fn tenant_requires_mfa(&self, tenant_id: &str) -> Result<bool, String> {
        let rel = self.mfa_policies_model.relation.clone();
        let tenant_col = self.mfa_policies_model.q("tenant_id");
        let require_col = self.mfa_policies_model.q("require_mfa");
        let value: Option<bool> = sqlx::query_scalar(&format!(
            "SELECT {require_col} FROM {rel} WHERE {tenant_col} = $1"
        ))
        .bind(tenant_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| format!("get mfa policy failed: {err}"))?;
        Ok(value.unwrap_or(false))
    }

    async fn set_user_phone(&self, user_id: &str, phone: &str) -> Result<(), String> {
        let mut assignments = std::collections::BTreeMap::new();
        assignments.insert(
            "phone".to_string(),
            LogicalAssignment::Set {
                value: LogicalValue::String(phone.to_string()),
            },
        );
        assignments.insert(
            "phone_verified_at".to_string(),
            LogicalAssignment::Set {
                value: LogicalValue::Null,
            },
        );
        assignments.insert("updated_at".to_string(), LogicalAssignment::ServerNow);
        execute_typed_update_on(
            &self.pool,
            LogicalUpdate {
                message_type: USER_MSG.to_string(),
                filter: eq("user_id", LogicalValue::String(user_id.to_string())),
                assignments,
                return_fields: Vec::new(),
                require_affected: false,
            },
        )
        .await?;
        Ok(())
    }

    async fn mark_phone_verified(&self, user_id: &str, now_unix: u64) -> Result<(), String> {
        let mut assignments = std::collections::BTreeMap::new();
        assignments.insert(
            "phone_verified_at".to_string(),
            LogicalAssignment::Set {
                value: logical_ts(now_unix)?,
            },
        );
        assignments.insert("updated_at".to_string(), LogicalAssignment::ServerNow);
        execute_typed_update_on(
            &self.pool,
            LogicalUpdate {
                message_type: USER_MSG.to_string(),
                filter: eq("user_id", LogicalValue::String(user_id.to_string())),
                assignments,
                return_fields: Vec::new(),
                require_affected: false,
            },
        )
        .await?;
        Ok(())
    }
}

impl PostgresUserStore {
    async fn get_user_by(&self, column: &str, value: &str) -> Result<Option<UserRecord>, String> {
        // A non-UUID value can never match the UUID user_id column - return
        // "no such user" instead of letting the $1::UUID cast surface as a raw
        // DB INTERNAL (22P02), so callers produce their typed precondition
        // errors rather than an opaque 500.
        if column == "user_id" && uuid::Uuid::parse_str(value.trim()).is_err() {
            return Ok(None);
        }
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

    async fn get_user_by_in_tenant(
        &self,
        column: &str,
        value: &str,
        tenant_id: &str,
    ) -> Result<Option<UserRecord>, String> {
        if tenant_id.trim().is_empty() {
            return Ok(None);
        }
        // A non-UUID value can never match the UUID user_id column - return
        // "no such user" instead of letting the $1::UUID cast surface as a raw
        // DB INTERNAL (22P02), so callers produce their typed precondition
        // errors rather than an opaque 500.
        if column == "user_id" && uuid::Uuid::parse_str(value.trim()).is_err() {
            return Ok(None);
        }
        let rel = self.users_relation();
        let col = self.users_model.q(column);
        let tenant_col = self.users_model.q("tenant_id");
        let projection = self.user_select_projection();
        let deleted_at = self.users_model.q("deleted_at");
        let value_expr = if column == "user_id" {
            "$1::UUID"
        } else {
            "$1"
        };
        let row = sqlx::query(&format!(
            "SELECT {projection} FROM {rel} WHERE {col} = {value_expr} AND {tenant_col} = $2 AND {deleted_at} IS NULL"
        ))
        .bind(value)
        .bind(tenant_id.trim())
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| format!("get tenant-scoped user failed: {err}"))?;
        row.as_ref()
            .map(user_from_row)
            .transpose()
            .map_err(|err| format!("decode tenant-scoped user failed: {err}"))
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

/// Password complexity policy, configurable via env — makes real the contract
/// the authn proto advertises ("Min 10 chars; 1 upper, 1 lower, 1 digit, 1
/// special"). Centralized so every password-setting RPC enforces the same rules
/// instead of an ad-hoc length check per call site.
#[derive(Debug, Clone)]
pub struct PasswordPolicy {
    pub min_length: usize,
    pub max_length: usize,
    pub require_upper: bool,
    pub require_lower: bool,
    pub require_digit: bool,
    pub require_symbol: bool,
}

impl Default for PasswordPolicy {
    fn default() -> Self {
        Self {
            min_length: 10,
            max_length: 1024,
            require_upper: true,
            require_lower: true,
            require_digit: true,
            require_symbol: true,
        }
    }
}

impl PasswordPolicy {
    /// Read the policy from the environment, falling back to the secure default.
    /// Knobs: `UDB_PASSWORD_MIN_LENGTH`, `UDB_PASSWORD_MAX_LENGTH`,
    /// `UDB_PASSWORD_REQUIRE_{UPPER,LOWER,DIGIT,SYMBOL}`.
    pub fn from_env() -> Self {
        let d = Self::default();
        Self {
            min_length: password_env_usize("UDB_PASSWORD_MIN_LENGTH", d.min_length),
            max_length: password_env_usize("UDB_PASSWORD_MAX_LENGTH", d.max_length),
            require_upper: password_env_flag("UDB_PASSWORD_REQUIRE_UPPER", d.require_upper),
            require_lower: password_env_flag("UDB_PASSWORD_REQUIRE_LOWER", d.require_lower),
            require_digit: password_env_flag("UDB_PASSWORD_REQUIRE_DIGIT", d.require_digit),
            require_symbol: password_env_flag("UDB_PASSWORD_REQUIRE_SYMBOL", d.require_symbol),
        }
    }

    /// Validate `password`, returning a human-readable reason on rejection.
    pub fn validate(&self, password: &str) -> Result<(), String> {
        let len = password.chars().count();
        if len < self.min_length {
            return Err(format!(
                "password must be at least {} characters",
                self.min_length
            ));
        }
        if len > self.max_length {
            return Err(format!(
                "password must be at most {} characters",
                self.max_length
            ));
        }
        if self.require_upper && !password.chars().any(char::is_uppercase) {
            return Err("password must contain an uppercase letter".to_string());
        }
        if self.require_lower && !password.chars().any(char::is_lowercase) {
            return Err("password must contain a lowercase letter".to_string());
        }
        if self.require_digit && !password.chars().any(|c| c.is_ascii_digit()) {
            return Err("password must contain a digit".to_string());
        }
        if self.require_symbol
            && !password
                .chars()
                .any(|c| !c.is_alphanumeric() && !c.is_whitespace())
        {
            return Err("password must contain a symbol".to_string());
        }
        Ok(())
    }
}

fn password_env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default)
}

fn password_env_flag(key: &str, default: bool) -> bool {
    match std::env::var(key) {
        Ok(v) => matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        ),
        Err(_) => default,
    }
}

/// Best-effort outbound OTP delivery to the operator's channel gateway via a
/// generic HTTP webhook. The operator sets `UDB_OTP_DELIVERY_WEBHOOK_URL` (and
/// optionally `UDB_OTP_DELIVERY_AUTH_HEADER`) to their email/SMS relay; UDB POSTs
/// `{channel, address, code, otp_type, user_id}` as JSON. Delivery never fails
/// OTP issuance — the OTP is already persisted and the caller holds the otp_id —
/// so a gateway outage degrades to "not delivered" rather than blocking auth.
/// With no webhook configured, delivery is skipped with a warning (the code is
/// still retrievable via the otp_id in dev/test).
pub async fn deliver_otp(channel: &str, address: &str, code: &str, otp_type: i32, user_id: &str) {
    let Some(url) = std::env::var("UDB_OTP_DELIVERY_WEBHOOK_URL")
        .ok()
        .filter(|u| !u.trim().is_empty())
    else {
        tracing::warn!(
            channel,
            "OTP delivery webhook not configured (UDB_OTP_DELIVERY_WEBHOOK_URL); OTP persisted but not sent"
        );
        return;
    };
    #[cfg(feature = "http-client")]
    {
        let mut builder = otp_delivery_client()
            .post(url.trim())
            .json(&serde_json::json!({
                "channel": channel,
                "address": address,
                "code": code,
                "otp_type": otp_type,
                "user_id": user_id,
            }));
        if let Ok(auth) = std::env::var("UDB_OTP_DELIVERY_AUTH_HEADER") {
            if !auth.trim().is_empty() {
                builder = builder.header("authorization", auth);
            }
        }
        match builder.send().await {
            Ok(resp) if resp.status().is_success() => {
                tracing::info!(channel, "OTP delivered via webhook");
            }
            Ok(resp) => tracing::error!(
                status = %resp.status(),
                channel,
                "OTP delivery webhook returned non-success status"
            ),
            Err(err) => {
                tracing::error!(error = %err, channel, "OTP delivery webhook request failed");
            }
        }
    }
    #[cfg(not(feature = "http-client"))]
    {
        let _ = (address, code, otp_type, user_id);
        tracing::error!(
            channel,
            "OTP delivery webhook configured but this build lacks the http-client feature"
        );
    }
}

#[cfg(feature = "http-client")]
fn otp_delivery_client() -> &'static reqwest::Client {
    static CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
    CLIENT.get_or_init(reqwest::Client::new)
}

pub fn hash_otp_code(code: &str, hash_key: &[u8]) -> String {
    hash_secret(&format!("otp:{}", code.trim()), hash_key)
}

pub fn verify_otp_code(code: &str, hash_key: &[u8], stored_hash: &str) -> bool {
    !stored_hash.is_empty() && constant_time_eq(&hash_otp_code(code, hash_key), stored_hash)
}

/// Keyed digest of an MFA recovery code; consumed by matching this hash against
/// stored rows (never stored or compared in plaintext).
pub fn hash_recovery_code(code: &str, hash_key: &[u8]) -> String {
    hash_secret(&format!("recovery:{}", code.trim()), hash_key)
}

#[cfg(test)]
mod tests;
