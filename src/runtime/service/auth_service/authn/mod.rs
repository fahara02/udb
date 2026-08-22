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

use crate::runtime::DataBrokerRuntime;
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
    authn_principal_to_pb_with_attributes, bounded_page_response, bounded_page_window,
    principal_from_api_key, principal_from_session, public_session_handle_from_hash,
    session_record_to_pb, timestamp_from_unix,
};
use super::now_unix;
use crate::ir::{
    ComparisonOp, LogicalAssignment, LogicalDelete, LogicalFilter, LogicalRecord, LogicalUpdate,
    LogicalValue,
};

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

fn authn_capability_status(
    operation: &'static str,
    capability_required: &'static str,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::capability_status(
        "authn",
        operation,
        capability_required,
        message,
    )
}

// The status constructors below with `cfg_attr(not(feature = "webauthn"),
// allow(dead_code))` have all their SERVING callers inside `webauthn`-gated
// handler regions, while unconditional typed-detail pin tests (mod tests,
// ~:4033+) keep their ErrorDetail contracts compiled in webauthn-off builds.
// The allowance is scoped to exactly that configuration so a webauthn-on
// build still detects rot if a serving caller disappears.
#[cfg_attr(not(feature = "webauthn"), allow(dead_code))]
fn authn_policy_status(
    operation: &'static str,
    policy_decision_id: &'static str,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::policy_status(operation, policy_decision_id, message)
}

fn authn_permission_policy_status(
    operation: impl Into<String>,
    policy_decision_id: impl Into<String>,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::policy_status_with_code(
        tonic::Code::PermissionDenied,
        operation,
        policy_decision_id,
        message,
    )
}

fn authn_internal_status(operation: impl Into<String>, message: impl Into<String>) -> Status {
    crate::runtime::executor_utils::internal_status("authn", operation, message)
}

fn authn_schema_not_found_status(
    operation: &'static str,
    schema_code: &'static str,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::schema_status(
        tonic::Code::NotFound,
        "authn",
        operation,
        schema_code,
        message,
    )
}

#[cfg_attr(not(feature = "webauthn"), allow(dead_code))]
fn authn_schema_already_exists_status(
    operation: &'static str,
    schema_code: &'static str,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::schema_status(
        tonic::Code::AlreadyExists,
        "authn",
        operation,
        schema_code,
        message,
    )
}

fn authn_user_not_found_status() -> Status {
    authn_schema_not_found_status("user_lookup", "authn_user_not_found", "user not found")
}

fn authn_otp_not_found_status() -> Status {
    authn_schema_not_found_status("otp_lookup", "authn_otp_not_found", "otp not found")
}

fn authn_device_not_found_status() -> Status {
    authn_schema_not_found_status(
        "device_revoke",
        "authn_device_not_found_or_already_revoked",
        "device not found or already revoked",
    )
}

fn native_authz_denied_status(
    action: &str,
    resource_name: &str,
    decision_id: impl Into<String>,
    deny_reason: &str,
) -> Status {
    authn_permission_policy_status(
        "authn_native_rpc_authorize",
        decision_id,
        format!("action '{action}' on '{resource_name}' denied by authz policy ({deny_reason})"),
    )
}

#[cfg_attr(not(feature = "webauthn"), allow(dead_code))]
fn webauthn_config_capability_status(
    operation: &'static str,
    capability_required: &'static str,
    message: impl Into<String>,
) -> Status {
    authn_capability_status(operation, capability_required, message)
}

#[cfg(feature = "webauthn")]
fn webauthn_attestation_capability_status(
    operation: &'static str,
    capability_required: &'static str,
    message: impl Into<String>,
) -> Status {
    authn_capability_status(operation, capability_required, message)
}

#[cfg(feature = "webauthn")]
fn webauthn_attestation_roots_status(message: impl Into<String>) -> Status {
    webauthn_attestation_capability_status(
        "webauthn_attestation_trust",
        "webauthn_attestation_roots",
        message,
    )
}

#[cfg(feature = "webauthn")]
fn webauthn_attestation_crypto_status(message: impl Into<String>) -> Status {
    webauthn_attestation_capability_status(
        "webauthn_attestation_crypto",
        "webauthn_attestation_crypto",
        message,
    )
}

#[cfg(feature = "webauthn")]
fn webauthn_policy_status(
    operation: &'static str,
    policy_decision_id: &'static str,
    message: impl Into<String>,
) -> Status {
    authn_policy_status(operation, policy_decision_id, message)
}

#[cfg_attr(not(feature = "webauthn"), allow(dead_code))]
fn webauthn_permission_policy_status(
    operation: &'static str,
    policy_decision_id: &'static str,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::policy_status_with_code(
        tonic::Code::PermissionDenied,
        operation,
        policy_decision_id,
        message,
    )
}

#[cfg_attr(not(feature = "webauthn"), allow(dead_code))]
fn webauthn_invalid_ceremony_status(operation: &'static str) -> Status {
    webauthn_permission_policy_status(
        operation,
        "invalid_webauthn_ceremony",
        "invalid WebAuthn ceremony",
    )
}

#[cfg_attr(not(feature = "webauthn"), allow(dead_code))]
fn webauthn_user_tenant_mismatch_status(operation: &'static str) -> Status {
    webauthn_permission_policy_status(
        operation,
        "tenant_id_user_mismatch",
        "tenant_id does not match user",
    )
}

#[cfg_attr(not(feature = "webauthn"), allow(dead_code))]
fn webauthn_user_project_mismatch_status(operation: &'static str) -> Status {
    webauthn_permission_policy_status(
        operation,
        "project_id_user_mismatch",
        "project_id does not match user",
    )
}

#[cfg(feature = "webauthn")]
fn webauthn_attestation_chain_untrusted_status() -> Status {
    webauthn_policy_status(
        "webauthn_attestation_trust",
        "webauthn_attestation_chain_not_trusted",
        "WebAuthn policy: attestation certificate chain is not trusted",
    )
}

#[cfg_attr(not(feature = "webauthn"), allow(dead_code))]
fn oidc_provider_disabled_status() -> Status {
    authn_policy_status(
        "oidc_authenticate",
        "identity_provider_disabled",
        "identity provider is disabled for this tenant",
    )
}

#[cfg_attr(not(feature = "webauthn"), allow(dead_code))]
fn oidc_jwks_url_invalid_status(err: impl std::fmt::Display) -> Status {
    authn_capability_status(
        "oidc_provider_config",
        "oidc_jwks_url",
        format!("configured OIDC jwks_url is invalid: {err}"),
    )
}

#[cfg_attr(not(feature = "webauthn"), allow(dead_code))]
fn webauthn_passkeys_required_status() -> Status {
    authn_policy_status(
        "webauthn_authentication",
        "webauthn_passkey_required",
        "user has no registered WebAuthn passkeys",
    )
}

fn webauthn_feature_required_status() -> Status {
    authn_capability_status(
        "webauthn_rpc",
        "webauthn_feature",
        "WebAuthn requires building UDB with the `webauthn` feature",
    )
}

#[cfg(feature = "webauthn")]
fn webauthn_attestation_invalid_field_status(
    field: &'static str,
    description: &'static str,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::invalid_argument_fields(message, [(field, description)])
}

// Topic submodules: each holds inherent `impl AuthnServiceImpl` blocks + helpers;
// the single `impl AuthnService` trait block below delegates to them.
mod core;
// Master-plan 3.4: signing-key source abstraction (env → KeyProvider trait,
// optional KMS provider). Selected once by `UDB_SIGNING_KEY_PROVIDER`.
mod key_provider;
mod lifecycle;
mod login;
mod mfa;
mod sessions;
mod signing_keys;
mod token_family;
mod tokens;
// Q#9-11: dev-only software WebAuthn authenticator (UDB_WEBAUTHN_TEST_MODE).
#[cfg(feature = "webauthn")]
mod webauthn_softauth;

/// `AuthnService` handler over the UDB-owned authn primitives.
#[derive(Clone)]
pub struct AuthnServiceImpl {
    sessions: Arc<dyn SessionStore>,
    api_keys: Arc<dyn ApiKeyStore>,
    users: Arc<dyn UserStore>,
    config: AuthnConfig,
    security: SecurityConfig,
    pg_pool: Option<PgPool>,
    /// Runtime handle for the typed native-entity authn path. Production wiring
    /// attaches this from `DataBrokerService`; direct test construction may leave
    /// it absent and continue through the store/pool fallbacks those tests own.
    runtime: Option<Arc<DataBrokerRuntime>>,
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
    // Q#9-11: the verbatim b64url challenge issued at Start, retained so the
    // dev soft-authenticator (UDB_WEBAUTHN_TEST_MODE) can build matching
    // clientDataJSON at Finish. `default` keeps old persisted envelopes loadable.
    #[serde(default)]
    challenge: String,
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
fn webauthn_policy_model() -> NativeModel {
    native_model(
        "udb.core.authn.entity.v1.WebAuthnPolicy",
        &[
            "policy_id",
            "tenant_id",
            "required_user_verification",
            "required_resident_key",
            "allowed_attestation_conveyance",
            "created_at",
            "updated_at",
        ],
    )
}

// ── Per-tenant WebAuthn policy enforcement (masterplan 4.1, policy half) ──────
//
// Gates passkey registration + assertion on the authenticator's ALREADY-PARSED
// self-reported flags (UV flag, credProps.rk, attestation conveyance/fmt), and
// validates attestation x5c certificate paths with vendored OpenSSL when a
// tenant demands non-"none" attestation. Packed, TPM, Android Key, and FIDO U2F
// x5c statement signatures are verified here.

/// Per-field WebAuthn requirement level (W3C `UserVerificationRequirement` /
/// resident-key vocabulary). Only `Required` can produce a deny; `Preferred` and
/// `Discouraged` never reject.
#[cfg(feature = "webauthn")]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum WebAuthnRequirement {
    Required,
    Preferred,
    Discouraged,
}

#[cfg(feature = "webauthn")]
impl WebAuthnRequirement {
    fn parse(raw: &str) -> Self {
        match raw.trim().to_ascii_lowercase().as_str() {
            "required" | "1" | "true" | "yes" => Self::Required,
            "discouraged" | "0" | "false" | "no" => Self::Discouraged,
            // Unknown / empty / "preferred" → preferred (the W3C default; never denies).
            _ => Self::Preferred,
        }
    }
}

/// Resolved per-tenant WebAuthn ceremony policy. Built from a `webauthn_policies`
/// row when present, else from the deployment env defaults (`UDB_WEBAUTHN_*`),
/// else from safe built-in defaults.
#[cfg(feature = "webauthn")]
#[derive(Clone, Debug)]
pub(super) struct WebAuthnPolicy {
    user_verification: WebAuthnRequirement,
    resident_key: WebAuthnRequirement,
    /// Lowercased conveyance tokens the RP accepts (subset of
    /// none/indirect/direct/enterprise). Empty ⇒ treated as `{none}`.
    allowed_conveyance: Vec<String>,
}

#[cfg(feature = "webauthn")]
impl Default for WebAuthnPolicy {
    /// Historical default policy: UV `preferred`, RK `discouraged`, conveyance
    /// `none` — identical to `from_parts(None, None, None)`.
    fn default() -> Self {
        Self::from_parts(None, None, None)
    }
}

#[cfg(feature = "webauthn")]
impl WebAuthnPolicy {
    fn parse_conveyance(raw: &str) -> Vec<String> {
        raw.split([',', ' ', ';'])
            .map(|t| t.trim().to_ascii_lowercase())
            .filter(|t| !t.is_empty())
            .collect()
    }

    /// Build from explicit column/option strings (a DB row or env). `None` ⇒ the
    /// field default.
    pub(super) fn from_parts(
        user_verification: Option<String>,
        resident_key: Option<String>,
        allowed_conveyance: Option<String>,
    ) -> Self {
        let allowed = allowed_conveyance
            .as_deref()
            .map(Self::parse_conveyance)
            .filter(|set| !set.is_empty())
            .unwrap_or_else(|| vec!["none".to_string()]);
        Self {
            user_verification: WebAuthnRequirement::parse(
                user_verification.as_deref().unwrap_or("preferred"),
            ),
            resident_key: WebAuthnRequirement::parse(
                resident_key.as_deref().unwrap_or("discouraged"),
            ),
            allowed_conveyance: allowed,
        }
    }

    /// Deployment-level default from the existing `UDB_WEBAUTHN_*` env vars (the
    /// same vars `webauthn()` reads). When unset, the result preserves historical
    /// behavior (UV preferred — webauthn-rs still requires UV for passkeys; RK not
    /// required; attestation "none" accepted).
    fn from_env() -> Self {
        let var = |key: &str| {
            std::env::var(key)
                .ok()
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
        };
        Self::from_parts(
            var("UDB_WEBAUTHN_USER_VERIFICATION"),
            var("UDB_WEBAUTHN_REQUIRE_RESIDENT_KEY"),
            var("UDB_WEBAUTHN_ATTESTATION"),
        )
    }

    /// Whether the RP demands a real attestation statement (the allowed set omits
    /// "none").
    fn requires_attestation(&self) -> bool {
        !self.allowed_conveyance.iter().any(|t| t == "none")
    }
}

/// Map an attestation `fmt` to a coarse conveyance bucket: "none" ⇒ "none"; any
/// signed statement ⇒ "direct". We cannot distinguish indirect/direct/enterprise
/// without parsing the attestation statement (the OpenSSL follow-up), so the gate
/// is the meaningful one: "attestation demanded but absent".
#[cfg(feature = "webauthn")]
fn conveyance_for_fmt(fmt: &str) -> &'static str {
    if fmt.eq_ignore_ascii_case("none") {
        "none"
    } else {
        "direct"
    }
}

/// Enforce the per-tenant policy against a freshly-VERIFIED registration
/// credential, using data webauthn-rs already parsed plus attestation validation:
///   * attestation `fmt`, `attStmt.x5c`, `attStmt.alg`, `attStmt.sig`,
///     and TPM `attStmt.certInfo`/`attStmt.pubArea`, from the
///     attestationObject CBOR;
///   * `credProps.rk`, from the unsigned client-extension outputs;
///   * the UV flag, from the authenticator-data flags byte.
///
/// Fails closed with `FailedPrecondition` on any violation. This validates x5c
/// chains for packed/tpm/android-key/fido-u2f when attestation is required, then
/// verifies packed, TPM, Android Key, and FIDO U2F attestation signatures over
/// their format-defined signed inputs.
#[cfg(feature = "webauthn")]
pub(super) fn enforce_registration_policy(
    policy: &WebAuthnPolicy,
    credential: &webauthn_rs::prelude::RegisterPublicKeyCredential,
) -> Result<(), Status> {
    let attestation = parse_attestation_object(credential.response.attestation_object.as_ref())
        .ok_or_else(|| {
            webauthn_attestation_invalid_field_status(
                "attestationObject",
                "must decode as a WebAuthn attestationObject CBOR map",
                "WebAuthn policy: unparseable attestationObject \
                     (cannot evaluate conveyance/UV)",
            )
        })?;

    let fmt = attestation.fmt.as_str();

    if attestation.auth_data.len() < 37 {
        return Err(webauthn_attestation_invalid_field_status(
            "authData",
            "must be at least 37 bytes to evaluate WebAuthn user verification",
            "WebAuthn policy: malformed authenticator data (cannot evaluate UV)",
        ));
    }

    // Attestation conveyance: when the tenant demands attestation, reject a
    // self-attested ("none") authenticator and validate the x5c path for
    // attested formats. Trust anchors are deployment-owned and explicit.
    if policy.requires_attestation() {
        let bucket = conveyance_for_fmt(fmt);
        if !policy.allowed_conveyance.iter().any(|t| t == bucket) {
            return Err(webauthn_policy_status(
                "webauthn_registration_policy",
                "attestation_conveyance_not_allowed",
                format!(
                    "WebAuthn policy: attestation conveyance '{fmt}' not permitted (tenant allows: {})",
                    policy.allowed_conveyance.join(",")
                ),
            ));
        }
        if bucket != "none" {
            verify_attestation_certificate_chain(fmt, &attestation.x5c_der)?;
            verify_attestation_statement_signature(
                fmt,
                &attestation,
                credential.response.client_data_json.as_ref(),
            )?;
        }
    }

    // Resident (discoverable) key. credProps.rk is UNSIGNED (a browser hint) — fine
    // for a fail-closed gate, never for trust.
    if policy.resident_key == WebAuthnRequirement::Required {
        let rk = credential
            .extensions
            .cred_props
            .as_ref()
            .and_then(|c| c.rk)
            .unwrap_or(false);
        if !rk {
            return Err(webauthn_policy_status(
                "webauthn_registration_policy",
                "resident_key_required",
                "WebAuthn policy: tenant requires a resident (discoverable) key but the \
                 authenticator did not report credProps.rk = true",
            ));
        }
    }

    // User verification at registration (authData flags bit 0x04).
    if policy.user_verification == WebAuthnRequirement::Required
        && !authenticator_data_user_verified(&attestation.auth_data)
    {
        return Err(webauthn_policy_status(
            "webauthn_registration_policy",
            "registration_user_verification_required",
            "WebAuthn policy: tenant requires user verification but the registration \
             authenticator-data UV flag was not set",
        ));
    }
    Ok(())
}

/// Enforce the per-tenant policy at ASSERTION time. The only authenticator-reported
/// signal at assertion is the UV flag (webauthn-rs surfaces it via
/// `AuthenticationResult::user_verified`). Fails closed with `FailedPrecondition`.
#[cfg(feature = "webauthn")]
pub(super) fn enforce_assertion_policy(
    policy: &WebAuthnPolicy,
    user_verified: bool,
) -> Result<(), Status> {
    if policy.user_verification == WebAuthnRequirement::Required && !user_verified {
        return Err(webauthn_policy_status(
            "webauthn_assertion_policy",
            "assertion_user_verification_required",
            "WebAuthn policy: tenant requires user verification but the assertion reported \
             UV = false",
        ));
    }
    Ok(())
}

/// `true` if the authenticator-data UV flag (0x04) is set. authData layout is
/// `rpIdHash(32) || flags(1) || signCount(4) || …`; the flags byte is at index 32.
#[cfg(feature = "webauthn")]
fn authenticator_data_user_verified(auth_data: &[u8]) -> bool {
    auth_data.get(32).is_some_and(|flags| flags & 0x04 != 0)
}

#[cfg(feature = "webauthn")]
const WEBAUTHN_ATTESTATION_ROOTS_PEM_ENV: &str = "UDB_WEBAUTHN_ATTESTATION_ROOTS_PEM";
#[cfg(feature = "webauthn")]
const WEBAUTHN_ATTESTATION_ROOTS_PEM_PATH_ENV: &str = "UDB_WEBAUTHN_ATTESTATION_ROOTS_PEM_PATH";
#[cfg(feature = "webauthn")]
const WEBAUTHN_SUPPORTED_ATTESTATION_FORMATS: &[&str] =
    &["packed", "tpm", "android-key", "fido-u2f"];

/// Validate the attestation x5c path with OpenSSL for formats that carry X.509
/// attestation chains. This prevents trusting a bare `fmt` string or
/// unvalidated certificate blob when a tenant requires attestation.
#[cfg(feature = "webauthn")]
fn verify_attestation_certificate_chain(fmt: &str, x5c_der: &[Vec<u8>]) -> Result<(), Status> {
    if !WEBAUTHN_SUPPORTED_ATTESTATION_FORMATS
        .iter()
        .any(|supported| fmt.eq_ignore_ascii_case(supported))
    {
        return Err(crate::runtime::executor_utils::invalid_argument_fields(
            format!(
                "WebAuthn policy: attestation format '{fmt}' is not supported for OpenSSL chain \
                 validation"
            ),
            [(
                "fmt",
                "must be packed, tpm, android-key, or fido-u2f for attestation chain validation",
            )],
        ));
    }
    if x5c_der.is_empty() {
        return Err(webauthn_attestation_invalid_field_status(
            "attStmt.x5c",
            "must include at least one X.509 certificate for attestation chain validation",
            format!("WebAuthn policy: attestation format '{fmt}' did not include attStmt.x5c"),
        ));
    }
    let roots = load_webauthn_attestation_roots()?;
    if roots.is_empty() {
        return Err(webauthn_attestation_roots_status(format!(
            "WebAuthn policy: attestation format '{fmt}' requires configured trust roots \
             ({WEBAUTHN_ATTESTATION_ROOTS_PEM_ENV} or {WEBAUTHN_ATTESTATION_ROOTS_PEM_PATH_ENV})"
        )));
    }
    openssl_verify_x509_chain(x5c_der, roots)
}

#[cfg(feature = "webauthn")]
fn load_webauthn_attestation_roots() -> Result<Vec<openssl::x509::X509>, Status> {
    let inline = std::env::var(WEBAUTHN_ATTESTATION_ROOTS_PEM_ENV)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty());
    let pem = match inline {
        Some(pem) => pem.into_bytes(),
        None => {
            let path = std::env::var(WEBAUTHN_ATTESTATION_ROOTS_PEM_PATH_ENV)
                .ok()
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty());
            match path {
                Some(path) => std::fs::read(&path).map_err(|err| {
                    webauthn_attestation_roots_status(format!(
                        "WebAuthn policy: read attestation roots PEM failed from {path}: {err}"
                    ))
                })?,
                None => return Ok(Vec::new()),
            }
        }
    };
    openssl::x509::X509::stack_from_pem(&pem).map_err(|err| {
        webauthn_attestation_roots_status(format!(
            "WebAuthn policy: parse attestation roots PEM failed: {err}"
        ))
    })
}

#[cfg(feature = "webauthn")]
fn openssl_verify_x509_chain(
    x5c_der: &[Vec<u8>],
    roots: Vec<openssl::x509::X509>,
) -> Result<(), Status> {
    use openssl::stack::Stack;
    use openssl::x509::store::X509StoreBuilder;
    use openssl::x509::{X509, X509StoreContext};

    let leaf = X509::from_der(&x5c_der[0]).map_err(|err| {
        webauthn_attestation_invalid_field_status(
            "attStmt.x5c",
            "must contain a valid DER-encoded attestation leaf certificate",
            format!("WebAuthn policy: parse attestation leaf certificate failed: {err}"),
        )
    })?;
    let mut chain = Stack::new().map_err(|err| {
        webauthn_attestation_crypto_status(format!(
            "WebAuthn policy: create attestation chain stack failed: {err}"
        ))
    })?;
    for der in &x5c_der[1..] {
        let cert = X509::from_der(der).map_err(|err| {
            webauthn_attestation_invalid_field_status(
                "attStmt.x5c",
                "must contain valid DER-encoded attestation intermediate certificates",
                format!(
                    "WebAuthn policy: parse attestation intermediate certificate failed: {err}"
                ),
            )
        })?;
        chain.push(cert).map_err(|err| {
            webauthn_attestation_crypto_status(format!(
                "WebAuthn policy: push attestation intermediate failed: {err}"
            ))
        })?;
    }

    let mut store = X509StoreBuilder::new().map_err(|err| {
        webauthn_attestation_crypto_status(format!(
            "WebAuthn policy: create attestation trust store failed: {err}"
        ))
    })?;
    for root in roots {
        store.add_cert(root).map_err(|err| {
            webauthn_attestation_crypto_status(format!(
                "WebAuthn policy: add attestation trust root failed: {err}"
            ))
        })?;
    }
    let store = store.build();
    let mut context = X509StoreContext::new().map_err(|err| {
        webauthn_attestation_crypto_status(format!(
            "WebAuthn policy: create attestation chain context failed: {err}"
        ))
    })?;
    let valid = context
        .init(&store, &leaf, &chain, |ctx| ctx.verify_cert())
        .map_err(|err| {
            webauthn_attestation_crypto_status(format!(
                "WebAuthn policy: attestation certificate chain validation failed: {err}"
            ))
        })?;
    if !valid {
        return Err(webauthn_attestation_chain_untrusted_status());
    }
    Ok(())
}

#[cfg(feature = "webauthn")]
fn verify_attestation_statement_signature(
    fmt: &str,
    attestation: &ParsedAttestationObject,
    client_data_json: &[u8],
) -> Result<(), Status> {
    if fmt.eq_ignore_ascii_case("packed") {
        return verify_packed_attestation_signature(attestation, client_data_json);
    }
    if fmt.eq_ignore_ascii_case("fido-u2f") {
        return verify_fido_u2f_attestation_signature(attestation, client_data_json);
    }
    if fmt.eq_ignore_ascii_case("android-key") {
        return verify_android_key_attestation_signature(attestation, client_data_json);
    }
    if fmt.eq_ignore_ascii_case("tpm") {
        return verify_tpm_attestation_signature(attestation, client_data_json);
    }
    Err(crate::runtime::executor_utils::invalid_argument_fields(
        format!(
            "WebAuthn policy: attestation format '{fmt}' is not supported for statement \
             signature verification"
        ),
        [(
            "fmt",
            "must be packed, tpm, android-key, or fido-u2f for attestation signature verification",
        )],
    ))
}

#[cfg(feature = "webauthn")]
fn verify_packed_attestation_signature(
    attestation: &ParsedAttestationObject,
    client_data_json: &[u8],
) -> Result<(), Status> {
    use openssl::sign::Verifier;
    use openssl::x509::X509;

    let sig = attestation.sig.as_ref().ok_or_else(|| {
        webauthn_attestation_invalid_field_status(
            "attStmt.sig",
            "must be present for packed attestation signature verification",
            "WebAuthn policy: packed attestation missing attStmt.sig",
        )
    })?;
    if sig.is_empty() {
        return Err(webauthn_attestation_invalid_field_status(
            "attStmt.sig",
            "must be a well-formed packed attestation statement signature",
            "WebAuthn policy: verify packed attestation signature failed: signature is empty",
        ));
    }
    let alg = attestation.alg.ok_or_else(|| {
        webauthn_attestation_invalid_field_status(
            "attStmt.alg",
            "must be present for packed attestation signature verification",
            "WebAuthn policy: packed attestation missing attStmt.alg",
        )
    })?;
    let digest = attestation_digest_for_alg("packed", alg)?;
    let leaf = attestation.x5c_der.first().ok_or_else(|| {
        webauthn_attestation_invalid_field_status(
            "attStmt.x5c",
            "must include the packed attestation leaf certificate",
            "WebAuthn policy: packed attestation missing attStmt.x5c",
        )
    })?;
    let cert = X509::from_der(leaf).map_err(|err| {
        webauthn_attestation_invalid_field_status(
            "attStmt.x5c",
            "must contain a valid DER-encoded packed attestation leaf certificate",
            format!("WebAuthn policy: parse packed attestation leaf certificate failed: {err}"),
        )
    })?;
    let pkey = cert.public_key().map_err(|err| {
        webauthn_attestation_crypto_status(format!(
            "WebAuthn policy: extract packed attestation public key failed: {err}"
        ))
    })?;

    let signed = webauthn_basic_attestation_signed_data(attestation, client_data_json);

    let mut verifier = Verifier::new(digest, &pkey).map_err(|err| {
        webauthn_attestation_crypto_status(format!(
            "WebAuthn policy: create packed attestation verifier failed: {err}"
        ))
    })?;
    verifier.update(&signed).map_err(|err| {
        webauthn_attestation_crypto_status(format!(
            "WebAuthn policy: feed packed attestation verifier failed: {err}"
        ))
    })?;
    if verifier.verify(sig).map_err(|err| {
        webauthn_attestation_invalid_field_status(
            "attStmt.sig",
            "must be a well-formed packed attestation statement signature",
            format!("WebAuthn policy: verify packed attestation signature failed: {err}"),
        )
    })? {
        Ok(())
    } else {
        Err(webauthn_attestation_invalid_field_status(
            "attStmt.sig",
            "must verify the packed attestation statement signature",
            "WebAuthn policy: packed attestation signature is invalid",
        ))
    }
}

#[cfg(feature = "webauthn")]
fn attestation_digest_for_alg(fmt: &str, alg: i64) -> Result<openssl::hash::MessageDigest, Status> {
    match alg {
        -7 | -257 => Ok(openssl::hash::MessageDigest::sha256()),
        -35 | -258 => Ok(openssl::hash::MessageDigest::sha384()),
        -36 | -259 => Ok(openssl::hash::MessageDigest::sha512()),
        _ => Err(crate::runtime::executor_utils::invalid_argument_fields(
            format!("WebAuthn policy: {fmt} attestation alg {alg} is not supported"),
            [(
                "attStmt.alg",
                "must be a supported COSE algorithm for WebAuthn attestation verification",
            )],
        )),
    }
}

#[cfg(feature = "webauthn")]
fn verify_tpm_attestation_signature(
    attestation: &ParsedAttestationObject,
    client_data_json: &[u8],
) -> Result<(), Status> {
    use openssl::sign::Verifier;
    use openssl::x509::X509;

    if attestation.ver.as_deref() != Some("2.0") {
        return Err(webauthn_attestation_invalid_field_status(
            "attStmt.ver",
            "must be \"2.0\" for TPM attestation signature verification",
            "WebAuthn policy: tpm attestation requires attStmt.ver = \"2.0\"",
        ));
    }
    let sig = attestation.sig.as_ref().ok_or_else(|| {
        webauthn_attestation_invalid_field_status(
            "attStmt.sig",
            "must be present for TPM attestation signature verification",
            "WebAuthn policy: tpm attestation missing attStmt.sig",
        )
    })?;
    if sig.is_empty() {
        return Err(webauthn_attestation_invalid_field_status(
            "attStmt.sig",
            "must be a well-formed TPM attestation statement signature",
            "WebAuthn policy: verify tpm attestation signature failed: signature is empty",
        ));
    }
    let alg = attestation.alg.ok_or_else(|| {
        webauthn_attestation_invalid_field_status(
            "attStmt.alg",
            "must be present for TPM attestation signature verification",
            "WebAuthn policy: tpm attestation missing attStmt.alg",
        )
    })?;
    let digest = attestation_digest_for_alg("tpm", alg)?;
    let cert_info = attestation.cert_info.as_ref().ok_or_else(|| {
        webauthn_attestation_invalid_field_status(
            "attStmt.certInfo",
            "must be present for TPM attestation binding verification",
            "WebAuthn policy: tpm attestation missing attStmt.certInfo",
        )
    })?;
    let pub_area = attestation.pub_area.as_ref().ok_or_else(|| {
        webauthn_attestation_invalid_field_status(
            "attStmt.pubArea",
            "must be present for TPM attestation name verification",
            "WebAuthn policy: tpm attestation missing attStmt.pubArea",
        )
    })?;

    let expected_extra = tpm_attestation_extra_data(attestation, client_data_json, alg)?;
    let certify_info = tpm_parse_certify_info(cert_info, &expected_extra)?;
    let pub_area_name = tpm_public_area_name(pub_area)?;
    if certify_info.attested_name != pub_area_name {
        return Err(webauthn_attestation_invalid_field_status(
            "attStmt.certInfo",
            "must name the same TPM public area as attStmt.pubArea",
            "WebAuthn policy: tpm attestation certInfo name does not match pubArea",
        ));
    }

    let leaf = attestation.x5c_der.first().ok_or_else(|| {
        webauthn_attestation_invalid_field_status(
            "attStmt.x5c",
            "must include the TPM attestation leaf certificate",
            "WebAuthn policy: tpm attestation missing attStmt.x5c",
        )
    })?;
    let cert = X509::from_der(leaf).map_err(|err| {
        webauthn_attestation_invalid_field_status(
            "attStmt.x5c",
            "must contain a valid DER-encoded TPM attestation leaf certificate",
            format!("WebAuthn policy: parse tpm attestation leaf certificate failed: {err}"),
        )
    })?;
    let pkey = cert.public_key().map_err(|err| {
        webauthn_attestation_crypto_status(format!(
            "WebAuthn policy: extract tpm attestation public key failed: {err}"
        ))
    })?;

    let mut verifier = Verifier::new(digest, &pkey).map_err(|err| {
        webauthn_attestation_crypto_status(format!(
            "WebAuthn policy: create tpm attestation verifier failed: {err}"
        ))
    })?;
    verifier.update(cert_info).map_err(|err| {
        webauthn_attestation_crypto_status(format!(
            "WebAuthn policy: feed tpm attestation verifier failed: {err}"
        ))
    })?;
    if verifier.verify(sig).map_err(|err| {
        webauthn_attestation_invalid_field_status(
            "attStmt.sig",
            "must be a well-formed TPM attestation statement signature",
            format!("WebAuthn policy: verify tpm attestation signature failed: {err}"),
        )
    })? {
        Ok(())
    } else {
        Err(webauthn_attestation_invalid_field_status(
            "attStmt.sig",
            "must verify the TPM attestation statement signature",
            "WebAuthn policy: tpm attestation signature is invalid",
        ))
    }
}

#[cfg(feature = "webauthn")]
#[derive(Debug)]
struct TpmCertifyInfo {
    attested_name: Vec<u8>,
}

#[cfg(feature = "webauthn")]
fn tpm_attestation_extra_data(
    attestation: &ParsedAttestationObject,
    client_data_json: &[u8],
    alg: i64,
) -> Result<Vec<u8>, Status> {
    let digest = attestation_digest_for_alg("tpm", alg)?;
    let signed = webauthn_basic_attestation_signed_data(attestation, client_data_json);
    let hashed = openssl::hash::hash(digest, &signed).map_err(|err| {
        webauthn_attestation_crypto_status(format!(
            "WebAuthn policy: hash tpm attestation signed data failed: {err}"
        ))
    })?;
    Ok(hashed.to_vec())
}

#[cfg(feature = "webauthn")]
fn tpm_parse_certify_info(
    cert_info: &[u8],
    expected_extra_data: &[u8],
) -> Result<TpmCertifyInfo, Status> {
    let mut pos = 0usize;
    let magic = tpm_read_u32("attStmt.certInfo", cert_info, &mut pos)?;
    if magic != 0xff54_4347 {
        return Err(webauthn_attestation_invalid_field_status(
            "attStmt.certInfo",
            "must contain a valid TPM2B certInfo structure",
            "WebAuthn policy: tpm certInfo magic is invalid",
        ));
    }
    let attestation_type = tpm_read_u16("attStmt.certInfo", cert_info, &mut pos)?;
    if attestation_type != 0x8017 {
        return Err(webauthn_attestation_invalid_field_status(
            "attStmt.certInfo",
            "must contain a TPM_ST_ATTEST_CERTIFY attestation type",
            "WebAuthn policy: tpm certInfo type is not TPM_ST_ATTEST_CERTIFY",
        ));
    }

    let _qualified_signer = tpm_read_sized_bytes("attStmt.certInfo", cert_info, &mut pos)?;
    let extra_data = tpm_read_sized_bytes("attStmt.certInfo", cert_info, &mut pos)?;
    if extra_data != expected_extra_data {
        return Err(webauthn_attestation_invalid_field_status(
            "attStmt.certInfo",
            "must bind to the authenticator data and clientDataHash",
            "WebAuthn policy: tpm certInfo extraData does not match authenticator/client data",
        ));
    }

    // TPMS_CLOCK_INFO (17 bytes) + firmwareVersion (8 bytes).
    pos = pos
        .checked_add(25)
        .filter(|p| *p <= cert_info.len())
        .ok_or_else(|| {
            webauthn_attestation_invalid_field_status(
                "attStmt.certInfo",
                "must include TPM clockInfo, firmwareVersion, and certify info",
                "WebAuthn policy: tpm certInfo truncated before certify info",
            )
        })?;

    let attested_name = tpm_read_sized_bytes("attStmt.certInfo", cert_info, &mut pos)?;
    let _qualified_name = tpm_read_sized_bytes("attStmt.certInfo", cert_info, &mut pos)?;
    Ok(TpmCertifyInfo { attested_name })
}

#[cfg(feature = "webauthn")]
fn tpm_public_area_name(pub_area: &[u8]) -> Result<Vec<u8>, Status> {
    if pub_area.len() < 4 {
        return Err(webauthn_attestation_invalid_field_status(
            "attStmt.pubArea",
            "must include a TPM public area nameAlg",
            "WebAuthn policy: tpm pubArea is truncated before nameAlg",
        ));
    }
    let name_alg = u16::from_be_bytes([pub_area[2], pub_area[3]]);
    let digest = tpm_name_digest_for_alg(name_alg)?;
    let hash = openssl::hash::hash(digest, pub_area).map_err(|err| {
        webauthn_attestation_crypto_status(format!(
            "WebAuthn policy: hash tpm pubArea name failed: {err}"
        ))
    })?;
    let mut name = Vec::with_capacity(2 + hash.len());
    name.extend_from_slice(&name_alg.to_be_bytes());
    name.extend_from_slice(&hash);
    Ok(name)
}

#[cfg(feature = "webauthn")]
fn tpm_name_digest_for_alg(alg: u16) -> Result<openssl::hash::MessageDigest, Status> {
    match alg {
        0x000b => Ok(openssl::hash::MessageDigest::sha256()),
        0x000c => Ok(openssl::hash::MessageDigest::sha384()),
        0x000d => Ok(openssl::hash::MessageDigest::sha512()),
        _ => Err(crate::runtime::executor_utils::invalid_argument_fields(
            format!("WebAuthn policy: tpm pubArea nameAlg 0x{alg:04x} is not supported"),
            [(
                "attStmt.pubArea",
                "must use SHA-256, SHA-384, or SHA-512 as the TPM nameAlg",
            )],
        )),
    }
}

#[cfg(feature = "webauthn")]
fn tpm_read_u16(field: &'static str, buf: &[u8], pos: &mut usize) -> Result<u16, Status> {
    let bytes: [u8; 2] = buf
        .get(*pos..pos.checked_add(2).unwrap_or(usize::MAX))
        .ok_or_else(|| {
            webauthn_attestation_invalid_field_status(
                field,
                "must contain a complete TPM structure",
                "WebAuthn policy: tpm structure truncated",
            )
        })?
        .try_into()
        .map_err(|_| {
            webauthn_attestation_invalid_field_status(
                field,
                "must contain a well-formed TPM u16",
                "WebAuthn policy: malformed tpm u16",
            )
        })?;
    *pos += 2;
    Ok(u16::from_be_bytes(bytes))
}

#[cfg(feature = "webauthn")]
fn tpm_read_u32(field: &'static str, buf: &[u8], pos: &mut usize) -> Result<u32, Status> {
    let bytes: [u8; 4] = buf
        .get(*pos..pos.checked_add(4).unwrap_or(usize::MAX))
        .ok_or_else(|| {
            webauthn_attestation_invalid_field_status(
                field,
                "must contain a complete TPM structure",
                "WebAuthn policy: tpm structure truncated",
            )
        })?
        .try_into()
        .map_err(|_| {
            webauthn_attestation_invalid_field_status(
                field,
                "must contain a well-formed TPM u32",
                "WebAuthn policy: malformed tpm u32",
            )
        })?;
    *pos += 4;
    Ok(u32::from_be_bytes(bytes))
}

#[cfg(feature = "webauthn")]
fn tpm_read_sized_bytes(
    field: &'static str,
    buf: &[u8],
    pos: &mut usize,
) -> Result<Vec<u8>, Status> {
    let len = tpm_read_u16(field, buf, pos)? as usize;
    let end = pos
        .checked_add(len)
        .filter(|p| *p <= buf.len())
        .ok_or_else(|| {
            webauthn_attestation_invalid_field_status(
                field,
                "must contain a complete TPM sized buffer",
                "WebAuthn policy: tpm sized buffer is truncated",
            )
        })?;
    let value = buf[*pos..end].to_vec();
    *pos = end;
    Ok(value)
}

#[cfg(feature = "webauthn")]
fn verify_android_key_attestation_signature(
    attestation: &ParsedAttestationObject,
    client_data_json: &[u8],
) -> Result<(), Status> {
    use openssl::sign::Verifier;
    use openssl::x509::X509;

    let sig = attestation.sig.as_ref().ok_or_else(|| {
        webauthn_attestation_invalid_field_status(
            "attStmt.sig",
            "must be present for android-key attestation signature verification",
            "WebAuthn policy: android-key attestation missing attStmt.sig",
        )
    })?;
    if sig.is_empty() {
        return Err(webauthn_attestation_invalid_field_status(
            "attStmt.sig",
            "must be a well-formed android-key attestation statement signature",
            "WebAuthn policy: verify android-key attestation signature failed: signature is empty",
        ));
    }
    let alg = attestation.alg.ok_or_else(|| {
        webauthn_attestation_invalid_field_status(
            "attStmt.alg",
            "must be present for android-key attestation signature verification",
            "WebAuthn policy: android-key attestation missing attStmt.alg",
        )
    })?;
    let digest = attestation_digest_for_alg("android-key", alg)?;
    let leaf = attestation.x5c_der.first().ok_or_else(|| {
        webauthn_attestation_invalid_field_status(
            "attStmt.x5c",
            "must include the android-key attestation leaf certificate",
            "WebAuthn policy: android-key attestation missing attStmt.x5c",
        )
    })?;
    let signed = webauthn_basic_attestation_signed_data(attestation, client_data_json);
    let cert = X509::from_der(leaf).map_err(|err| {
        webauthn_attestation_invalid_field_status(
            "attStmt.x5c",
            "must contain a valid DER-encoded android-key attestation leaf certificate",
            format!(
                "WebAuthn policy: parse android-key attestation leaf certificate failed: {err}"
            ),
        )
    })?;
    let pkey = cert.public_key().map_err(|err| {
        webauthn_attestation_crypto_status(format!(
            "WebAuthn policy: extract android-key attestation public key failed: {err}"
        ))
    })?;

    let mut verifier = Verifier::new(digest, &pkey).map_err(|err| {
        webauthn_attestation_crypto_status(format!(
            "WebAuthn policy: create android-key attestation verifier failed: {err}"
        ))
    })?;
    verifier.update(&signed).map_err(|err| {
        webauthn_attestation_crypto_status(format!(
            "WebAuthn policy: feed android-key attestation verifier failed: {err}"
        ))
    })?;
    if verifier.verify(sig).map_err(|err| {
        webauthn_attestation_invalid_field_status(
            "attStmt.sig",
            "must be a well-formed android-key attestation statement signature",
            format!("WebAuthn policy: verify android-key attestation signature failed: {err}"),
        )
    })? {
        Ok(())
    } else {
        Err(webauthn_attestation_invalid_field_status(
            "attStmt.sig",
            "must verify the android-key attestation statement signature",
            "WebAuthn policy: android-key attestation signature is invalid",
        ))
    }
}

#[cfg(feature = "webauthn")]
fn webauthn_basic_attestation_signed_data(
    attestation: &ParsedAttestationObject,
    client_data_json: &[u8],
) -> Vec<u8> {
    use sha2::{Digest, Sha256};

    let mut signed = Vec::with_capacity(attestation.auth_data.len() + 32);
    signed.extend_from_slice(&attestation.auth_data);
    signed.extend_from_slice(&Sha256::digest(client_data_json));
    signed
}

#[cfg(feature = "webauthn")]
fn verify_fido_u2f_attestation_signature(
    attestation: &ParsedAttestationObject,
    client_data_json: &[u8],
) -> Result<(), Status> {
    use openssl::hash::MessageDigest;
    use openssl::sign::Verifier;
    use openssl::x509::X509;

    let sig = attestation.sig.as_ref().ok_or_else(|| {
        webauthn_attestation_invalid_field_status(
            "attStmt.sig",
            "must be present for FIDO U2F attestation signature verification",
            "WebAuthn policy: fido-u2f attestation missing attStmt.sig",
        )
    })?;
    if sig.is_empty() {
        return Err(webauthn_attestation_invalid_field_status(
            "attStmt.sig",
            "must be a well-formed FIDO U2F attestation statement signature",
            "WebAuthn policy: verify fido-u2f attestation signature failed: signature is empty",
        ));
    }
    let leaf = attestation.x5c_der.first().ok_or_else(|| {
        webauthn_attestation_invalid_field_status(
            "attStmt.x5c",
            "must include the FIDO U2F attestation leaf certificate",
            "WebAuthn policy: fido-u2f attestation missing attStmt.x5c",
        )
    })?;
    let signed = fido_u2f_attestation_verification_data(attestation, client_data_json)?;
    let cert = X509::from_der(leaf).map_err(|err| {
        webauthn_attestation_invalid_field_status(
            "attStmt.x5c",
            "must contain a valid DER-encoded FIDO U2F attestation leaf certificate",
            format!("WebAuthn policy: parse fido-u2f attestation leaf certificate failed: {err}"),
        )
    })?;
    let pkey = cert.public_key().map_err(|err| {
        webauthn_attestation_crypto_status(format!(
            "WebAuthn policy: extract fido-u2f attestation public key failed: {err}"
        ))
    })?;

    let mut verifier = Verifier::new(MessageDigest::sha256(), &pkey).map_err(|err| {
        webauthn_attestation_crypto_status(format!(
            "WebAuthn policy: create fido-u2f attestation verifier failed: {err}"
        ))
    })?;
    verifier.update(&signed).map_err(|err| {
        webauthn_attestation_crypto_status(format!(
            "WebAuthn policy: feed fido-u2f attestation verifier failed: {err}"
        ))
    })?;
    if verifier.verify(sig).map_err(|err| {
        webauthn_attestation_invalid_field_status(
            "attStmt.sig",
            "must be a well-formed FIDO U2F attestation statement signature",
            format!("WebAuthn policy: verify fido-u2f attestation signature failed: {err}"),
        )
    })? {
        Ok(())
    } else {
        Err(webauthn_attestation_invalid_field_status(
            "attStmt.sig",
            "must verify the FIDO U2F attestation statement signature",
            "WebAuthn policy: fido-u2f attestation signature is invalid",
        ))
    }
}

#[cfg(feature = "webauthn")]
fn fido_u2f_attestation_verification_data(
    attestation: &ParsedAttestationObject,
    client_data_json: &[u8],
) -> Result<Vec<u8>, Status> {
    use sha2::{Digest, Sha256};

    let auth_data = attestation.auth_data.as_slice();
    if auth_data.len() < 37 {
        return Err(webauthn_attestation_invalid_field_status(
            "authData",
            "must be at least 37 bytes for FIDO U2F attestation verification",
            "WebAuthn policy: fido-u2f attestation has malformed authenticator data",
        ));
    }
    if auth_data[32] & 0x40 == 0 {
        return Err(webauthn_attestation_invalid_field_status(
            "authData.flags",
            "must include attested credential data for FIDO U2F attestation verification",
            "WebAuthn policy: fido-u2f attestation missing attested credential data",
        ));
    }

    let rp_id_hash = auth_data.get(0..32).ok_or_else(|| {
        webauthn_attestation_invalid_field_status(
            "authData.rpIdHash",
            "must be present in FIDO U2F authenticator data",
            "WebAuthn policy: fido-u2f attestation missing rpIdHash",
        )
    })?;
    let mut pos = 37usize;
    auth_data.get(pos..pos + 16).ok_or_else(|| {
        webauthn_attestation_invalid_field_status(
            "authData.aaguid",
            "must include credential AAGUID for FIDO U2F attestation verification",
            "WebAuthn policy: fido-u2f attestation missing credential AAGUID",
        )
    })?;
    pos += 16;
    let credential_id_len_bytes: [u8; 2] = auth_data
        .get(pos..pos + 2)
        .ok_or_else(|| {
            webauthn_attestation_invalid_field_status(
                "authData.credentialIdLength",
                "must include credential id length for FIDO U2F attestation verification",
                "WebAuthn policy: fido-u2f attestation missing credential id length",
            )
        })?
        .try_into()
        .map_err(|_| {
            webauthn_attestation_invalid_field_status(
                "authData.credentialIdLength",
                "must be a two-byte FIDO U2F credential id length",
                "WebAuthn policy: fido-u2f attestation malformed credential id length",
            )
        })?;
    pos += 2;
    let credential_id_len = u16::from_be_bytes(credential_id_len_bytes) as usize;
    let credential_id = auth_data.get(pos..pos + credential_id_len).ok_or_else(|| {
        webauthn_attestation_invalid_field_status(
            "authData.credentialId",
            "must include the full credential id for FIDO U2F attestation verification",
            "WebAuthn policy: fido-u2f attestation truncated credential id",
        )
    })?;
    pos += credential_id_len;
    let cose_key = cbor_read_cose_ec2_es256_public_key(auth_data, &mut pos).ok_or_else(|| {
        webauthn_attestation_invalid_field_status(
            "authData.credentialPublicKey",
            "must decode as an EC2/P-256/ES256 COSE credential public key",
            "WebAuthn policy: fido-u2f attestation requires an EC2/P-256/ES256 credential key",
        )
    })?;

    let mut signed = Vec::with_capacity(1 + 32 + 32 + credential_id.len() + 65);
    signed.push(0x00);
    signed.extend_from_slice(rp_id_hash);
    signed.extend_from_slice(&Sha256::digest(client_data_json));
    signed.extend_from_slice(credential_id);
    signed.push(0x04);
    signed.extend_from_slice(&cose_key.x);
    signed.extend_from_slice(&cose_key.y);
    Ok(signed)
}

#[cfg(feature = "webauthn")]
struct CoseEc2PublicKey {
    x: [u8; 32],
    y: [u8; 32],
}

// ── Minimal CBOR reader (attestationObject conveyance/UV extraction) ──────────
// WebAuthn attestationObjects are definite-length CBOR maps (CTAP2 canonical), so
// a tiny walker suffices to pull `fmt` (text), `authData` (bytes), and optional
// `attStmt.x5c` certificate DER blobs, `attStmt.alg`/`sig`, TPM
// `attStmt.ver`/`certInfo`/`pubArea`, and COSE EC2 credential keys needed by
// statement verification. This is NOT a general CBOR codec.

#[cfg(feature = "webauthn")]
struct ParsedAttestationObject {
    fmt: String,
    auth_data: Vec<u8>,
    ver: Option<String>,
    alg: Option<i64>,
    sig: Option<Vec<u8>>,
    x5c_der: Vec<Vec<u8>>,
    cert_info: Option<Vec<u8>>,
    pub_area: Option<Vec<u8>>,
}

#[cfg(feature = "webauthn")]
#[derive(Default)]
struct ParsedAttestationStatement {
    ver: Option<String>,
    alg: Option<i64>,
    sig: Option<Vec<u8>>,
    x5c_der: Vec<Vec<u8>>,
    cert_info: Option<Vec<u8>>,
    pub_area: Option<Vec<u8>>,
}

/// Returns parsed fields from a top-level attestationObject map, or `None`.
#[cfg(feature = "webauthn")]
fn parse_attestation_object(buf: &[u8]) -> Option<ParsedAttestationObject> {
    let mut pos = 0usize;
    let count = cbor_read_map_header(buf, &mut pos)?;
    let mut fmt: Option<String> = None;
    let mut auth_data: Option<Vec<u8>> = None;
    let mut att_stmt = ParsedAttestationStatement::default();
    for _ in 0..count {
        let key = cbor_read_text(buf, &mut pos)?;
        match key.as_str() {
            "fmt" => fmt = Some(cbor_read_text(buf, &mut pos)?),
            "attStmt" => att_stmt = cbor_read_att_stmt(buf, &mut pos)?,
            "authData" => auth_data = Some(cbor_read_bytes(buf, &mut pos)?),
            _ => cbor_skip_value(buf, &mut pos)?,
        }
    }
    Some(ParsedAttestationObject {
        fmt: fmt?,
        auth_data: auth_data?,
        ver: att_stmt.ver,
        alg: att_stmt.alg,
        sig: att_stmt.sig,
        x5c_der: att_stmt.x5c_der,
        cert_info: att_stmt.cert_info,
        pub_area: att_stmt.pub_area,
    })
}

/// Read a definite-length argument from the `info` bits of a CBOR initial byte.
#[cfg(feature = "webauthn")]
fn cbor_read_len(buf: &[u8], pos: &mut usize, info: u8) -> Option<u64> {
    match info {
        0..=23 => Some(info as u64),
        24 => {
            let v = *buf.get(*pos)? as u64;
            *pos += 1;
            Some(v)
        }
        25 => {
            let v = u16::from_be_bytes([*buf.get(*pos)?, *buf.get(*pos + 1)?]) as u64;
            *pos += 2;
            Some(v)
        }
        26 => {
            let a: [u8; 4] = buf.get(*pos..*pos + 4)?.try_into().ok()?;
            *pos += 4;
            Some(u32::from_be_bytes(a) as u64)
        }
        27 => {
            let a: [u8; 8] = buf.get(*pos..*pos + 8)?.try_into().ok()?;
            *pos += 8;
            Some(u64::from_be_bytes(a))
        }
        _ => None, // indefinite-length items are not used by attestationObjects
    }
}

#[cfg(feature = "webauthn")]
fn cbor_read_map_header(buf: &[u8], pos: &mut usize) -> Option<u64> {
    let ib = *buf.get(*pos)?;
    *pos += 1;
    if ib >> 5 != 5 {
        return None; // major type 5 == map
    }
    cbor_read_len(buf, pos, ib & 0x1f)
}

#[cfg(feature = "webauthn")]
fn cbor_read_array_header(buf: &[u8], pos: &mut usize) -> Option<u64> {
    let ib = *buf.get(*pos)?;
    *pos += 1;
    if ib >> 5 != 4 {
        return None; // major type 4 == array
    }
    cbor_read_len(buf, pos, ib & 0x1f)
}

#[cfg(feature = "webauthn")]
fn cbor_read_text(buf: &[u8], pos: &mut usize) -> Option<String> {
    let ib = *buf.get(*pos)?;
    *pos += 1;
    if ib >> 5 != 3 {
        return None; // major type 3 == text string
    }
    let n = cbor_read_len(buf, pos, ib & 0x1f)? as usize;
    let slice = buf.get(*pos..pos.checked_add(n)?)?;
    *pos += n;
    std::str::from_utf8(slice).ok().map(str::to_string)
}

#[cfg(feature = "webauthn")]
fn cbor_read_bytes(buf: &[u8], pos: &mut usize) -> Option<Vec<u8>> {
    let ib = *buf.get(*pos)?;
    *pos += 1;
    if ib >> 5 != 2 {
        return None; // major type 2 == byte string
    }
    let n = cbor_read_len(buf, pos, ib & 0x1f)? as usize;
    let slice = buf.get(*pos..pos.checked_add(n)?)?;
    *pos += n;
    Some(slice.to_vec())
}

#[cfg(feature = "webauthn")]
fn cbor_read_i64(buf: &[u8], pos: &mut usize) -> Option<i64> {
    let ib = *buf.get(*pos)?;
    *pos += 1;
    let major = ib >> 5;
    let n = cbor_read_len(buf, pos, ib & 0x1f)?;
    match major {
        0 => i64::try_from(n).ok(),
        1 => {
            let n = i64::try_from(n).ok()?;
            Some(-1 - n)
        }
        _ => None,
    }
}

#[cfg(feature = "webauthn")]
fn cbor_read_att_stmt(buf: &[u8], pos: &mut usize) -> Option<ParsedAttestationStatement> {
    let count = cbor_read_map_header(buf, pos)?;
    let mut stmt = ParsedAttestationStatement::default();
    for _ in 0..count {
        let key = cbor_read_text(buf, pos)?;
        match key.as_str() {
            "ver" => stmt.ver = Some(cbor_read_text(buf, pos)?),
            "alg" => stmt.alg = Some(cbor_read_i64(buf, pos)?),
            "sig" => stmt.sig = Some(cbor_read_bytes(buf, pos)?),
            "certInfo" => stmt.cert_info = Some(cbor_read_bytes(buf, pos)?),
            "pubArea" => stmt.pub_area = Some(cbor_read_bytes(buf, pos)?),
            "x5c" => {
                let certs = cbor_read_array_header(buf, pos)?;
                for _ in 0..certs {
                    stmt.x5c_der.push(cbor_read_bytes(buf, pos)?);
                }
            }
            _ => cbor_skip_value(buf, pos)?,
        }
    }
    Some(stmt)
}

#[cfg(feature = "webauthn")]
fn cbor_read_cose_ec2_es256_public_key(buf: &[u8], pos: &mut usize) -> Option<CoseEc2PublicKey> {
    let count = cbor_read_map_header(buf, pos)?;
    let mut kty = None;
    let mut alg = None;
    let mut crv = None;
    let mut x = None;
    let mut y = None;
    for _ in 0..count {
        let key = cbor_read_i64(buf, pos)?;
        match key {
            1 => kty = Some(cbor_read_i64(buf, pos)?),
            3 => alg = Some(cbor_read_i64(buf, pos)?),
            -1 => crv = Some(cbor_read_i64(buf, pos)?),
            -2 => {
                let bytes = cbor_read_bytes(buf, pos)?;
                x = Some(bytes.try_into().ok()?);
            }
            -3 => {
                let bytes = cbor_read_bytes(buf, pos)?;
                y = Some(bytes.try_into().ok()?);
            }
            _ => cbor_skip_value(buf, pos)?,
        }
    }
    if kty == Some(2) && alg == Some(-7) && crv == Some(1) {
        Some(CoseEc2PublicKey { x: x?, y: y? })
    } else {
        None
    }
}

/// Skip exactly one CBOR data item (any major type, definite length).
#[cfg(feature = "webauthn")]
fn cbor_skip_value(buf: &[u8], pos: &mut usize) -> Option<()> {
    let ib = *buf.get(*pos)?;
    *pos += 1;
    let major = ib >> 5;
    let info = ib & 0x1f;
    match major {
        0 | 1 => {
            cbor_read_len(buf, pos, info)?;
        }
        2 | 3 => {
            let n = cbor_read_len(buf, pos, info)? as usize;
            *pos = pos.checked_add(n).filter(|p| *p <= buf.len())?;
        }
        4 => {
            let n = cbor_read_len(buf, pos, info)?;
            for _ in 0..n {
                cbor_skip_value(buf, pos)?;
            }
        }
        5 => {
            let n = cbor_read_len(buf, pos, info)?;
            for _ in 0..n {
                cbor_skip_value(buf, pos)?;
                cbor_skip_value(buf, pos)?;
            }
        }
        6 => {
            cbor_read_len(buf, pos, info)?;
            cbor_skip_value(buf, pos)?;
        }
        7 => match info {
            24 => *pos = pos.checked_add(1).filter(|p| *p <= buf.len())?,
            25 => *pos = pos.checked_add(2).filter(|p| *p <= buf.len())?,
            26 => *pos = pos.checked_add(4).filter(|p| *p <= buf.len())?,
            27 => *pos = pos.checked_add(8).filter(|p| *p <= buf.len())?,
            _ => {}
        },
        _ => return None,
    }
    Some(())
}

pub(super) fn authn_eq(field: &str, value: LogicalValue) -> LogicalFilter {
    LogicalFilter::Comparison {
        field: field.to_string(),
        op: ComparisonOp::Eq,
        value,
    }
}

pub(super) fn authn_cmp(field: &str, op: ComparisonOp, value: LogicalValue) -> LogicalFilter {
    LogicalFilter::Comparison {
        field: field.to_string(),
        op,
        value,
    }
}

pub(super) fn authn_and(filters: Vec<LogicalFilter>) -> LogicalFilter {
    LogicalFilter::And(filters)
}

pub(super) fn authn_record(
    fields: impl IntoIterator<Item = (&'static str, LogicalValue)>,
) -> LogicalRecord {
    fields
        .into_iter()
        .map(|(field, value)| (field.to_string(), value))
        .collect()
}

pub(super) fn authn_set(value: LogicalValue) -> LogicalAssignment {
    LogicalAssignment::Set { value }
}

pub(super) fn unix_to_utc(unix: u64) -> Option<chrono::DateTime<chrono::Utc>> {
    if unix == 0 {
        return None;
    }
    chrono::DateTime::<chrono::Utc>::from_timestamp(unix as i64, 0)
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

/// Test-only re-exports of the single dev-echo gate chokepoint so the served-path
/// conformance test (13.1.4.1) — which lives outside the `authn::mfa` module — can
/// assert the prod-closed property. `mfa::otp_dev_echo_*` are `pub(super)`, visible
/// only inside `authn`; this surfaces them at `authn::` (crate-visible like
/// `test_otp_codes`) without widening the production visibility.
#[cfg(test)]
pub(crate) use mfa::{otp_dev_echo_enabled, otp_dev_echo_resolved};

impl AuthnServiceImpl {
    pub fn new(config: AuthnConfig, security: SecurityConfig) -> Self {
        Self {
            sessions: Arc::new(UnavailableSessionStore),
            api_keys: Arc::new(UnavailableApiKeyStore),
            users: Arc::new(UnavailableUserStore),
            config,
            security,
            pg_pool: None,
            runtime: None,
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
        // Derive the Postgres pool from a Postgres-backed user store so the atomic
        // paths (create_user's user+OTP+event transaction, refresh-token families)
        // work when the service is built from stores alone — an explicit
        // `.with_postgres()` still overrides this. Fixes the inconsistent state
        // where Postgres stores were wired but `pg_pool` stayed `None`.
        let pg_pool = users.backing_pool();
        Self {
            sessions,
            api_keys,
            users,
            config,
            security,
            pg_pool,
            runtime: None,
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

    pub(crate) fn with_runtime(mut self, runtime: Option<Arc<DataBrokerRuntime>>) -> Self {
        self.runtime = runtime;
        self
    }

    pub(super) fn authn_runtime(&self) -> Result<&DataBrokerRuntime, Status> {
        self.runtime.as_deref().ok_or_else(|| {
            authn_capability_status(
                "native_entity_dispatch",
                "runtime_native_entity_dispatch",
                "this operation requires the native typed authn runtime",
            )
        })
    }

    pub(super) fn authn_context(&self, tenant_id: &str, project_id: &str) -> crate::RequestContext {
        crate::RequestContext {
            tenant_id: tenant_id.to_string(),
            project_id: project_id.to_string(),
            ..crate::RequestContext::default()
        }
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
        use crate::runtime::authz::{AuthzQuery, ResourceRef};

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
        // Casbin decision (policy/role/relationship match) — the same engine the
        // data plane and the AuthzService `check_access` path use. Casbin-only:
        // there is no legacy snapshot `authorize`; deny-by-default holds and an
        // engine error fails closed inside `casbin_authorize` (deny).
        let decision = snapshot.load().casbin_authorize(&query).await;
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
        Err(native_authz_denied_status(
            action,
            &resource_name,
            decision.decision_id.clone(),
            &decision.deny_reason,
        ))
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
            .map_err(|err| {
                authn_internal_status(
                    "emit_event_in_tx",
                    format!("audit event write failed for {topic}: {err}"),
                )
            })
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

    /// OTP/MFA at-rest hashing+sealing key. Fails CLOSED when unresolved: an
    /// empty secret would otherwise seal TOTP secrets / recovery codes under
    /// `SHA256("")` — a world-known constant — silently defeating MFA-at-rest.
    /// Mirrors the fail-closed posture the API-key (`hash_key`) and password
    /// (`password_hash_key`) paths already enforce at their point of use.
    pub(super) fn otp_hash_key(&self) -> Result<Vec<u8>, Status> {
        let secret = self.config.otp_hash_secret();
        if secret.trim().is_empty() {
            return Err(authn_capability_status(
                "otp_hashing",
                "otp_hash_secret",
                "MFA/OTP hashing requires UDB_OTP_HASH_SECRET (or UDB_SESSION_HASH_SECRET) \
                 to be configured; refusing to seal or verify OTP material under an empty key",
            ));
        }
        Ok(secret.as_bytes().to_vec())
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
    /// Validate that a client-supplied identifier is a well-formed UUID at the
    /// RPC boundary, returning `InvalidArgument` (never reaching the DB with a
    /// malformed id, never leaking a raw Postgres `22P02` error). bug_report.md B1.
    pub(super) fn require_uuid_arg(value: &str, field: &str) -> Result<(), Status> {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return Err(crate::runtime::executor_utils::invalid_argument_fields(
                format!("{field} is required"),
                [(field.to_string(), "must be a non-empty UUID".to_string())],
            ));
        }
        Uuid::parse_str(trimmed).map(|_| ()).map_err(|_| {
            crate::runtime::executor_utils::invalid_argument_fields(
                format!("{field} must be a valid UUID"),
                [(field.to_string(), "must be a valid UUID".to_string())],
            )
        })
    }

    #[cfg(any(feature = "webauthn", feature = "oidc", test))]
    fn authn_field_violation(
        field: impl Into<String>,
        description: impl Into<String>,
        message: impl Into<String>,
    ) -> Status {
        crate::runtime::executor_utils::invalid_argument_fields(
            message,
            [(field.into(), description.into())],
        )
    }

    #[cfg(any(feature = "webauthn", test))]
    fn webauthn_user_id_required_status() -> Status {
        Self::authn_field_violation(
            "user_id",
            "must be a non-empty user id",
            "user_id is required",
        )
    }

    #[cfg(any(feature = "webauthn", test))]
    fn webauthn_user_id_uuid_status() -> Status {
        Self::authn_field_violation(
            "user_id",
            "must be a valid UUID for WebAuthn ceremonies",
            "WebAuthn users must have UUID user_id",
        )
    }

    #[cfg(any(feature = "webauthn", test))]
    fn webauthn_challenge_id_invalid_status() -> Status {
        Self::authn_field_violation(
            "challenge_id",
            "must be a valid UUID",
            "challenge_id must be a valid UUID",
        )
    }

    #[cfg(any(feature = "webauthn", test))]
    fn validate_webauthn_finish_fields(
        challenge_id: &str,
        public_key_credential_json: &str,
    ) -> Result<(), Status> {
        let mut fields = Vec::new();
        if challenge_id.trim().is_empty() {
            fields.push(("challenge_id", "must be a non-empty WebAuthn challenge id"));
        }
        if public_key_credential_json.trim().is_empty() {
            fields.push((
                "public_key_credential_json",
                "must be a non-empty WebAuthn credential JSON payload",
            ));
        }
        if !fields.is_empty() {
            return Err(crate::runtime::executor_utils::invalid_argument_fields(
                "challenge_id and public_key_credential_json are required",
                fields,
            ));
        }
        Ok(())
    }

    #[cfg(any(feature = "webauthn", test))]
    fn invalid_webauthn_registration_credential_json_status(err: impl std::fmt::Display) -> Status {
        Self::authn_field_violation(
            "public_key_credential_json",
            "must decode as a WebAuthn registration credential",
            format!("invalid WebAuthn registration credential JSON: {}", err),
        )
    }

    #[cfg(any(feature = "webauthn", test))]
    fn invalid_webauthn_authentication_credential_json_status(
        err: impl std::fmt::Display,
    ) -> Status {
        Self::authn_field_violation(
            "public_key_credential_json",
            "must decode as a WebAuthn authentication credential",
            format!("invalid WebAuthn authentication credential JSON: {}", err),
        )
    }

    #[cfg(any(feature = "oidc", test))]
    fn oidc_id_token_required_status() -> Status {
        crate::runtime::executor_utils::invalid_argument_fields(
            "OIDC authentication requires external_token or bearer_token containing an ID token",
            [
                (
                    "external_token",
                    "must contain an OIDC ID token when bearer_token is empty",
                ),
                (
                    "bearer_token",
                    "must contain an OIDC ID token when external_token is empty",
                ),
            ],
        )
    }

    #[cfg(any(feature = "oidc", test))]
    fn oidc_issuer_required_status() -> Status {
        Self::authn_field_violation(
            "issuer",
            "must be supplied by the provider registry, request issuer, or UDB_OIDC_ISSUER",
            "OIDC issuer is required (provider registry, request issuer, or UDB_OIDC_ISSUER)",
        )
    }

    #[cfg(any(feature = "oidc", test))]
    fn oidc_client_id_required_status() -> Status {
        crate::runtime::executor_utils::invalid_argument_fields(
            "OIDC client_id/audience is required",
            [
                (
                    "client_id",
                    "must be supplied by the request, provider registry, or UDB_OIDC_CLIENT_ID",
                ),
                (
                    "audience",
                    "must be supplied when client_id is empty and no provider/default client is configured",
                ),
            ],
        )
    }

    #[cfg(any(feature = "oidc", test))]
    fn oidc_client_id_audience_mismatch_status() -> Status {
        crate::runtime::executor_utils::invalid_argument_fields(
            "OIDC client_id and audience must match",
            [
                ("client_id", "must match audience when both are supplied"),
                ("audience", "must match client_id when both are supplied"),
            ],
        )
    }

    #[cfg(any(feature = "oidc", test))]
    fn oidc_nonce_required_status() -> Status {
        Self::authn_field_violation(
            "attributes.nonce",
            "must include a non-empty OIDC nonce",
            "OIDC nonce is required in attributes[\"nonce\"]",
        )
    }

    #[cfg(any(feature = "oidc", test))]
    fn invalid_oidc_issuer_status(err: impl std::fmt::Display) -> Status {
        Self::authn_field_violation(
            "issuer",
            "must be a valid OIDC issuer URL",
            format!("invalid OIDC issuer: {err}"),
        )
    }

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
            Err(err) => Err(authn_internal_status("user_is_active", err)),
        }
    }

    pub(crate) async fn jwt_persisted_state_valid(
        &self,
        claims: &crate::runtime::security::SecurityClaims,
        now: u64,
    ) -> Result<bool, Status> {
        let is_service = claims
            .service_identity
            .as_deref()
            .is_some_and(|s| !s.is_empty());
        if is_service {
            let subject = claims.sub.as_deref().unwrap_or_default().trim();
            let tenant = claims.tenant_id.as_deref().unwrap_or_default().trim();
            let project = claims.project_id.as_deref().unwrap_or_default().trim();
            let Some(pool) = self.pg_pool.as_ref() else {
                return Err(authn_capability_status(
                    "jwt_service_grant_validation",
                    "native_postgres_auth_store",
                    "service bearer validation requires the durable typed-grant store",
                ));
            };
            let valid = super::grants::validate_service_principal_against_grant(
                pool,
                tenant,
                subject,
                project,
                claims.service_identity.as_deref().unwrap_or_default(),
                &claims.resolved_scopes(),
            )
            .await
            .map_err(|error| authn_internal_status("jwt_service_grant_validate", error))?;
            if !valid {
                return Ok(false);
            }
        } else if !self
            .user_is_active(claims.sub.as_deref().unwrap_or_default())
            .await?
        {
            return Ok(false);
        }
        // Phase 3 (I2.3): reject any token whose jti is on the durable cluster-wide
        // revocation deny list, before its natural expiry — across every node.
        if let Some(jti) = claims.jti.as_deref().filter(|j| !j.trim().is_empty())
            && self
                .is_token_revoked(
                    jti,
                    claims.tenant_id.as_deref().unwrap_or_default(),
                    claims.sub.as_deref().unwrap_or_default(),
                    claims.iat.unwrap_or(0).max(0) as u64,
                )
                .await
                .0
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
                Err(err) => return Err(authn_internal_status("jwt_session_validate", err)),
            }
        }
        Ok(true)
    }

    #[cfg(feature = "webauthn")]
    fn require_pg_pool(&self) -> Result<&PgPool, Status> {
        self.pg_pool.as_ref().ok_or_else(|| {
            authn_capability_status(
                "webauthn_credentials",
                "native_postgres_auth_store",
                "WebAuthn requires the native Postgres auth store to persist passkeys and challenges",
            )
        })
    }

    #[cfg(feature = "oidc")]
    pub(super) async fn authenticate_oidc_token(
        &self,
        req: &authn_pb::AuthnRequest,
    ) -> Result<Principal, Status> {
        use openidconnect::core::{
            CoreClient, CoreIdToken, CoreJsonWebKeySet, CoreProviderMetadata,
        };
        use openidconnect::reqwest;
        use openidconnect::{ClientId, ClientSecret, IssuerUrl, JsonWebKeySetUrl, Nonce};
        use std::str::FromStr;

        let id_token = if !req.external_token.trim().is_empty() {
            req.external_token.trim().to_string()
        } else {
            req.bearer_token.trim().to_string()
        };
        if id_token.is_empty() {
            return Err(Self::oidc_id_token_required_status());
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
                return Err(oidc_provider_disabled_status());
            }
        }
        let registered_issuer = registered
            .as_ref()
            .map(|p| p.issuer.clone())
            .unwrap_or_default();
        let registered_client_id = registered
            .as_ref()
            .and_then(|p| p.client_ids.first().cloned())
            .or_else(|| {
                registered
                    .as_ref()
                    .and_then(|p| p.audiences.first().cloned())
            })
            .unwrap_or_default();
        let registered_audiences = registered
            .as_ref()
            .map(|p| p.audiences.clone())
            .unwrap_or_default();
        let registered_claim_mapping_json = registered
            .as_ref()
            .map(|p| p.claim_mapping_json.clone())
            .unwrap_or_else(|| "{}".to_string());
        let registered_group_mapping_json = registered
            .as_ref()
            .map(|p| p.group_mapping_json.clone())
            .unwrap_or_else(|| "{}".to_string());
        let registered_jwks_url = registered
            .as_ref()
            .map(|p| p.jwks_url.clone())
            .unwrap_or_default();
        // IDP1 + discovery-SSRF: the issuer is the TRUST ANCHOR used to fetch the
        // JWKS and verify the id_token — it must come ONLY from the resolved
        // provider's registered issuer or the operator-configured UDB_OIDC_ISSUER,
        // NEVER from the client request. Otherwise an attacker names their own
        // issuer, hosts a matching `/.well-known/openid-configuration` + JWKS, and
        // self-signs a token asserting any tenant (full auth bypass on the public
        // Authenticate RPC) — and the discovery fetch itself becomes an SSRF to any
        // host. A client-supplied `req.issuer` is honored only if it exactly equals
        // the trusted value (so legitimate clients that echo the real issuer work).
        let trusted_issuer = if !registered_issuer.trim().is_empty() {
            registered_issuer.trim().to_string()
        } else {
            std::env::var("UDB_OIDC_ISSUER").unwrap_or_default()
        };
        if trusted_issuer.is_empty() {
            return Err(Self::oidc_issuer_required_status());
        }
        if !req.issuer.trim().is_empty() && req.issuer.trim() != trusted_issuer {
            return Err(Status::unauthenticated("invalid credential"));
        }
        let issuer = trusted_issuer;
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
            return Err(Self::oidc_client_id_required_status());
        }
        if !req.client_id.trim().is_empty()
            && !req.audience.trim().is_empty()
            && req.client_id.trim() != req.audience.trim()
        {
            return Err(Self::oidc_client_id_audience_mismatch_status());
        }
        if !req.audience.trim().is_empty()
            && !registered_audiences.is_empty()
            && !registered_audiences
                .iter()
                .any(|audience| audience == req.audience.trim())
        {
            return Err(Status::unauthenticated("invalid credential"));
        }
        let nonce = req
            .attributes
            .get("nonce")
            .cloned()
            .or_else(|| std::env::var("UDB_OIDC_NONCE").ok())
            .unwrap_or_default();
        if nonce.trim().is_empty() {
            return Err(Self::oidc_nonce_required_status());
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
                .map_err(|err| {
                    authn_internal_status(
                        "oidc_http_client_build",
                        format!("OIDC HTTP client build failed: {err}"),
                    )
                })?;
            let issuer_url = IssuerUrl::new(issuer).map_err(Self::invalid_oidc_issuer_status)?;
            let client_id = ClientId::new(client_id);
            let id_token = CoreIdToken::from_str(&id_token)
                .map_err(|_| Status::unauthenticated("invalid credential"))?;
            let nonce = Nonce::new(nonce);
            let (subject, claims_json) = if registered_jwks_url.trim().is_empty() {
                let client_secret = std::env::var("UDB_OIDC_CLIENT_SECRET")
                    .ok()
                    .filter(|secret| !secret.trim().is_empty())
                    .map(ClientSecret::new);
                let provider_metadata =
                    CoreProviderMetadata::discover(&issuer_url, &http_client)
                        .map_err(|_| Status::unauthenticated("invalid credential"))?;
                let client =
                    CoreClient::from_provider_metadata(provider_metadata, client_id, client_secret);
                let verifier = client.id_token_verifier();
                let claims = id_token
                    .claims(&verifier, &nonce)
                    .map_err(|_| Status::unauthenticated("invalid credential"))?;
                (
                    claims.subject().as_str().to_string(),
                    serde_json::to_value(claims).map_err(|err| {
                        authn_internal_status(
                            "oidc_claims_serialize",
                            format!("serialize OIDC claims failed: {err}"),
                        )
                    })?,
                )
            } else {
                let jwks_url = JsonWebKeySetUrl::new(registered_jwks_url)
                    .map_err(|err| oidc_jwks_url_invalid_status(err))?;
                let jwks = CoreJsonWebKeySet::fetch(&jwks_url, &http_client)
                    .map_err(|_| Status::unauthenticated("invalid credential"))?;
                let client = CoreClient::new(client_id, issuer_url, jwks);
                let verifier = client.id_token_verifier();
                let claims = id_token
                    .claims(&verifier, &nonce)
                    .map_err(|_| Status::unauthenticated("invalid credential"))?;
                (
                    claims.subject().as_str().to_string(),
                    serde_json::to_value(claims).map_err(|err| {
                        authn_internal_status(
                            "oidc_claims_serialize",
                            format!("serialize OIDC claims failed: {err}"),
                        )
                    })?,
                )
            };
            Ok(super::idp::oidc_principal_from_verified_claims(
                &provider_id,
                &subject,
                &tenant_id,
                &project_id,
                scopes,
                &registered_claim_mapping_json,
                &registered_group_mapping_json,
                &claims_json,
            ))
        })
        .await
        .map_err(|err| {
            authn_internal_status(
                "oidc_verification_task",
                format!("OIDC verification task failed: {err}"),
            )
        })?
    }

    #[cfg(feature = "webauthn")]
    fn webauthn_config(&self) -> Result<WebAuthnConfig, Status> {
        let rp_id = std::env::var("UDB_WEBAUTHN_RP_ID").unwrap_or_default();
        let rp_origin = std::env::var("UDB_WEBAUTHN_ORIGIN").unwrap_or_default();
        if rp_id.trim().is_empty() || rp_origin.trim().is_empty() {
            return Err(webauthn_config_capability_status(
                "webauthn_relying_party_config",
                "webauthn_rp_config",
                "WebAuthn requires UDB_WEBAUTHN_RP_ID and UDB_WEBAUTHN_ORIGIN",
            ));
        }
        // Fail-closed hardening (02.5.1.1): in production posture the RP must be a
        // real HTTPS origin with a non-loopback host, and the dev soft-authenticator
        // must never be reachable. Non-production behavior is unchanged.
        if SecurityConfig::current().is_production() {
            let origin_lower = rp_origin.trim().to_ascii_lowercase();
            if !origin_lower.starts_with("https://") {
                return Err(webauthn_config_capability_status(
                    "webauthn_relying_party_config",
                    "webauthn_https_origin",
                    "production WebAuthn requires an https UDB_WEBAUTHN_ORIGIN",
                ));
            }
            let host_is_loopback = |value: &str| {
                let host = value.trim().to_ascii_lowercase();
                host == "localhost" || host == "127.0.0.1" || host == "::1"
            };
            // origin host: strip scheme + any path/port to compare the bare host.
            let origin_host = origin_lower
                .trim_start_matches("https://")
                .split(['/', ':'])
                .next()
                .unwrap_or_default()
                .to_string();
            if host_is_loopback(&rp_id) || host_is_loopback(&origin_host) {
                return Err(webauthn_config_capability_status(
                    "webauthn_relying_party_config",
                    "webauthn_production_rp_host",
                    "production WebAuthn RP id/origin must not be localhost/127.0.0.1",
                ));
            }
            if webauthn_softauth::test_mode_enabled() {
                return Err(webauthn_config_capability_status(
                    "webauthn_relying_party_config",
                    "webauthn_softauth_disabled",
                    "production WebAuthn must not enable UDB_WEBAUTHN_TEST_MODE (dev soft-authenticator)",
                ));
            }
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
        // RP attestation / resident-key / user-verification policy (02.5.2.1).
        //
        // Two distinct halves:
        //   * REQUEST side (this builder): webauthn-rs 0.5 (the pinned build) does
        //     NOT expose builder setters for attestation conveyance, resident-key
        //     (discoverable credential) requirement, or user-verification — those are
        //     per-ceremony arguments (`start_attested_passkey_registration`, etc.),
        //     and the only related builder setter
        //     (`danger_set_user_presence_only_security_keys`) is gated behind a
        //     feature this build does NOT enable. So the plain passkey ceremony
        //     cannot REQUEST a non-default policy from the client.
        //   * RESPONSE side (masterplan 4.1): the per-tenant
        //     WebAuthn policy (UV / resident-key / attestation conveyance) IS
        //     enforced fail-closed at `finish_webauthn_registration_impl` /
        //     `finish_webauthn_authentication_impl` via `enforce_*_policy`, reading
        //     the authenticator's parsed flags and, for demanded attestation,
        //     validating attStmt.x5c against explicit OpenSSL trust roots and
        //     enforcing the packed / TPM / Android Key / FIDO U2F statement
        //     signatures.
        //
        // Because the request side cannot ask the client for attestation/RK, an
        // ENV-level policy that DEMANDS them would make every plain-passkey
        // registration unsatisfiable (the authenticator returns "none"); so we keep
        // failing closed in production when such an env policy is set, rather than
        // silently issuing ceremonies that can never complete. Per-tenant policies
        // (the `webauthn_policies` row) are evaluated on the response at finish.
        use std::time::Duration;
        use webauthn_rs::prelude::{Url, WebauthnBuilder};

        // Defaults: UV preferred, attestation none, resident-key not required.
        let policy_var = |key: &str| {
            std::env::var(key)
                .ok()
                .map(|v| v.trim().to_ascii_lowercase())
                .filter(|v| !v.is_empty())
        };
        let attestation = policy_var("UDB_WEBAUTHN_ATTESTATION");
        let resident_key = policy_var("UDB_WEBAUTHN_REQUIRE_RESIDENT_KEY");
        let user_verification = policy_var("UDB_WEBAUTHN_USER_VERIFICATION");

        let requests_non_default = attestation.as_deref().is_some_and(|v| v != "none")
            || resident_key
                .as_deref()
                .is_some_and(|v| matches!(v, "1" | "true" | "yes" | "required"))
            || user_verification
                .as_deref()
                .is_some_and(|v| v != "preferred");
        if requests_non_default && SecurityConfig::current().is_production() {
            return Err(webauthn_config_capability_status(
                "webauthn_policy_config",
                "webauthn_policy_builder_support",
                "requested WebAuthn attestation/resident-key/user-verification policy \
                 is not enforceable in this build (host-blocked on webauthn-rs 0.5 \
                 builder support); refusing to silently ignore it in production",
            ));
        }

        let cfg = self.webauthn_config()?;
        let origin = Url::parse(&cfg.rp_origin).map_err(|err| {
            webauthn_config_capability_status(
                "webauthn_relying_party_config",
                "webauthn_origin_url",
                format!("invalid WebAuthn origin: {err}"),
            )
        })?;
        WebauthnBuilder::new(&cfg.rp_id, &origin)
            .map_err(|err| {
                webauthn_config_capability_status(
                    "webauthn_relying_party_config",
                    "webauthn_rp_config",
                    format!("invalid WebAuthn RP config: {err:?}"),
                )
            })?
            .rp_name(&cfg.rp_name)
            .timeout(Duration::from_secs(cfg.challenge_ttl_secs))
            .build()
            .map_err(|err| {
                webauthn_config_capability_status(
                    "webauthn_relying_party_config",
                    "webauthn_config_builder",
                    format!("invalid WebAuthn config: {err:?}"),
                )
            })
    }

    /// Resolve the effective per-tenant WebAuthn policy (masterplan 4.1): a
    /// `webauthn_policies` row when one exists, else the deployment env baseline
    /// (`UDB_WEBAUTHN_*`). Best-effort by design — a missing pool / table / row
    /// falls back to the env baseline rather than failing the ceremony (an older
    /// deployment without the optional table still gets its env-level posture).
    /// The env baseline itself defaults to the historical safe posture, so the
    /// fallback never silently disables a stricter env-configured requirement.
    #[cfg(feature = "webauthn")]
    async fn resolve_webauthn_policy(&self, tenant_id: &str) -> WebAuthnPolicy {
        let env_default = WebAuthnPolicy::from_env();
        let Ok(pool) = self.require_pool() else {
            return env_default;
        };
        let m = webauthn_policy_model();
        let sql = format!(
            "SELECT {uv}, {rk}, {conv} FROM {rel} WHERE {tenant} = $1",
            uv = m.q("required_user_verification"),
            rk = m.q("required_resident_key"),
            conv = m.q("allowed_attestation_conveyance"),
            rel = m.relation,
            tenant = m.q("tenant_id"),
        );
        match sqlx::query(&sql).bind(tenant_id).fetch_optional(pool).await {
            Ok(Some(row)) => WebAuthnPolicy::from_parts(
                row.try_get::<String, _>(0).ok(),
                row.try_get::<String, _>(1).ok(),
                row.try_get::<String, _>(2).ok(),
            ),
            // No per-tenant override (or the optional table is absent on an older
            // deployment) → fall back to the deployment env baseline.
            _ => env_default,
        }
    }

    #[cfg(feature = "webauthn")]
    async fn store_webauthn_challenge(
        &self,
        user: &UserRecord,
        ceremony: &str,
        state_json: String,
        expires_at: u64,
    ) -> Result<String, Status> {
        let challenge_id = Uuid::new_v4().to_string();
        if let Ok(runtime) = self.authn_runtime() {
            let state = serde_json::from_str(&state_json).map_err(|err| {
                authn_internal_status(
                    "store_webauthn_challenge_decode_json",
                    format!("decode WebAuthn challenge JSON failed: {err}"),
                )
            })?;
            let expires_at = unix_to_utc(expires_at).ok_or_else(|| {
                authn_internal_status(
                    "store_webauthn_challenge_expiry",
                    "invalid WebAuthn challenge expiry",
                )
            })?;
            let context = self.authn_context(&user.tenant_id, &user.project_id);
            let record = authn_record([
                ("challenge_id", LogicalValue::String(challenge_id.clone())),
                ("user_id", LogicalValue::String(user.user_id.clone())),
                ("ceremony", LogicalValue::String(ceremony.to_string())),
                ("state_json", LogicalValue::Json(state)),
                ("tenant_id", LogicalValue::String(user.tenant_id.clone())),
                ("project_id", LogicalValue::String(user.project_id.clone())),
                ("expires_at", LogicalValue::Timestamp(expires_at)),
            ]);
            runtime
                .native_entity_write_for_service(
                    "authn",
                    &context,
                    "udb.core.authn.entity.v1.WebAuthnChallenge",
                    record,
                    crate::ir::ConflictStrategy::Error,
                )
                .await
                .map_err(|err| {
                    crate::runtime::executor_utils::prefix_status(
                        "store WebAuthn challenge failed",
                        err,
                    )
                })?;
            return Ok(challenge_id);
        }
        let pool = self.require_pg_pool()?;
        let model = webauthn_challenge_model();
        let rel = &model.relation;
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
        .map_err(|err| {
            crate::runtime::executor_utils::sqlx_error_to_status(
                "store WebAuthn challenge failed",
                &err,
            )
        })?;
        Ok(challenge_id)
    }

    #[cfg(feature = "webauthn")]
    async fn load_webauthn_challenge(
        &self,
        challenge_id: &str,
        ceremony: &str,
    ) -> Result<WebAuthnChallengeRecord, Status> {
        // bug_report.md §I: challenge_id is bound as `$1::UUID`; a non-UUID value made
        // Postgres reject the query and the raw error escaped as `Internal`. Validate at
        // the boundary so Finish{Registration,Authentication} return InvalidArgument and
        // never leak the DB error string.
        if uuid::Uuid::parse_str(challenge_id.trim()).is_err() {
            return Err(Self::webauthn_challenge_id_invalid_status());
        }
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
        .map_err(|err| {
            authn_internal_status(
                "load_webauthn_challenge_query",
                format!("load WebAuthn challenge failed: {err}"),
            )
        })?
        .ok_or_else(|| {
            webauthn_invalid_ceremony_status(match ceremony {
                "registration" => "finish_webauthn_registration",
                "authentication" => "finish_webauthn_authentication",
                _ => "webauthn_challenge",
            })
        })?;
        Ok(WebAuthnChallengeRecord {
            user_id: row.try_get("user_id").map_err(|err| {
                authn_internal_status(
                    "load_webauthn_challenge_user_id",
                    format!("decode WebAuthn challenge user_id failed: {err}"),
                )
            })?,
            state_json: row.try_get("state_json").map_err(|err| {
                authn_internal_status(
                    "load_webauthn_challenge_state_json",
                    format!("decode WebAuthn challenge state_json failed: {err}"),
                )
            })?,
            tenant_id: row.try_get("tenant_id").map_err(|err| {
                authn_internal_status(
                    "load_webauthn_challenge_tenant_id",
                    format!("decode WebAuthn challenge tenant_id failed: {err}"),
                )
            })?,
            project_id: row.try_get("project_id").map_err(|err| {
                authn_internal_status(
                    "load_webauthn_challenge_project_id",
                    format!("decode WebAuthn challenge project_id failed: {err}"),
                )
            })?,
        })
    }

    #[cfg(feature = "webauthn")]
    /// `tenant_id` is REQUIRED: the challenge row is a tenant-scoped message, and the
    /// IR compiler refuses to compile a query for one without a concrete tenant
    /// ("tenant_scope_required"). Passing an empty context made every WebAuthn finish
    /// fail INTERNAL at challenge consumption.
    async fn consume_webauthn_challenge(
        &self,
        tenant_id: &str,
        challenge_id: &str,
    ) -> Result<(), Status> {
        if let Ok(runtime) = self.authn_runtime() {
            let context = self.authn_context(tenant_id, "");
            let mut assignments = std::collections::BTreeMap::new();
            assignments.insert("consumed_at".to_string(), LogicalAssignment::ServerNow);
            let op = LogicalUpdate {
                message_type: "udb.core.authn.entity.v1.WebAuthnChallenge".to_string(),
                filter: authn_and(vec![
                    authn_eq(
                        "challenge_id",
                        LogicalValue::String(challenge_id.to_string()),
                    ),
                    LogicalFilter::IsNull("consumed_at".to_string()),
                ]),
                assignments,
                return_fields: Vec::new(),
                require_affected: false,
            };
            runtime
                .native_entity_update_for_service("authn", &context, op)
                .await
                .map_err(|err| {
                    authn_internal_status(
                        "consume_webauthn_challenge_runtime",
                        format!("consume WebAuthn challenge failed: {err}"),
                    )
                })?;
            return Ok(());
        }
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
        .map_err(|err| {
            authn_internal_status(
                "consume_webauthn_challenge_pg",
                format!("consume WebAuthn challenge failed: {err}"),
            )
        })?;
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
        .map_err(|err| {
            authn_internal_status(
                "load_webauthn_passkeys_query",
                format!("load WebAuthn passkeys failed: {err}"),
            )
        })?;
        rows.into_iter()
            .map(|row| {
                Ok(WebAuthnPasskeyRecord {
                    passkey_json: row.try_get("passkey_json").map_err(|err| {
                        authn_internal_status(
                            "load_webauthn_passkeys_decode_json",
                            format!("decode WebAuthn passkey_json failed: {err}"),
                        )
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
            authn_internal_status(
                "webauthn_credential_id_serialize",
                format!("serialize WebAuthn credential id failed: {err}"),
            )
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
        if let Ok(runtime) = self.authn_runtime() {
            let passkey = serde_json::from_str(&passkey_json).map_err(|err| {
                authn_internal_status(
                    "insert_webauthn_passkey_decode_json",
                    format!("decode WebAuthn passkey JSON failed: {err}"),
                )
            })?;
            let context = self.authn_context(&user.tenant_id, &user.project_id);
            let record = authn_record([
                (
                    "credential_id",
                    LogicalValue::String(credential_id.to_string()),
                ),
                ("user_id", LogicalValue::String(user.user_id.clone())),
                ("passkey_json", LogicalValue::Json(passkey)),
                ("label", LogicalValue::String(label.to_string())),
                ("tenant_id", LogicalValue::String(user.tenant_id.clone())),
                ("project_id", LogicalValue::String(user.project_id.clone())),
            ]);
            runtime
                .native_entity_write_for_service(
                    "authn",
                    &context,
                    "udb.core.authn.entity.v1.WebAuthnCredential",
                    record,
                    crate::ir::ConflictStrategy::Error,
                )
                .await
                .map_err(|err| {
                    authn_schema_already_exists_status(
                        "store_webauthn_passkey",
                        "webauthn_passkey_already_exists",
                        format!("store WebAuthn passkey failed: {err}"),
                    )
                })?;
            return Ok(());
        }
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
        .map_err(|err| {
            authn_schema_already_exists_status(
                "store_webauthn_passkey",
                "webauthn_passkey_already_exists",
                format!("store WebAuthn passkey failed: {err}"),
            )
        })?;
        Ok(())
    }

    #[cfg(feature = "webauthn")]
    /// `tenant_id` is REQUIRED: this update runs over the generic native-store seam,
    /// whose fail-closed tenant gate rejects an empty tenant with
    /// `tenant_scope_required`. Passing an empty context made every
    /// FinishWebAuthnAuthentication fail INTERNAL once the sign-count write landed.
    async fn update_webauthn_passkey_after_auth(
        &self,
        tenant_id: &str,
        credential_id: &str,
        passkey_json: String,
    ) -> Result<(), Status> {
        if let Ok(runtime) = self.authn_runtime() {
            let passkey = serde_json::from_str(&passkey_json).map_err(|err| {
                authn_internal_status(
                    "update_webauthn_passkey_decode_json",
                    format!("decode WebAuthn passkey JSON failed: {err}"),
                )
            })?;
            let context = self.authn_context(tenant_id, "");
            let mut assignments = std::collections::BTreeMap::new();
            assignments.insert(
                "passkey_json".to_string(),
                authn_set(LogicalValue::Json(passkey)),
            );
            assignments.insert("updated_at".to_string(), LogicalAssignment::ServerNow);
            assignments.insert("last_used_at".to_string(), LogicalAssignment::ServerNow);
            let op = LogicalUpdate {
                message_type: "udb.core.authn.entity.v1.WebAuthnCredential".to_string(),
                filter: authn_eq(
                    "credential_id",
                    LogicalValue::String(credential_id.to_string()),
                ),
                assignments,
                return_fields: Vec::new(),
                require_affected: true,
            };
            runtime
                .native_entity_update_for_service("authn", &context, op)
                .await
                .map_err(|err| {
                    authn_internal_status(
                        "update_webauthn_passkey_runtime",
                        format!("update WebAuthn passkey failed: {err}"),
                    )
                })?;
            return Ok(());
        }
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
        .map_err(|err| {
            authn_internal_status(
                "update_webauthn_passkey_pg",
                format!("update WebAuthn passkey failed: {err}"),
            )
        })?;
        Ok(())
    }

    #[cfg(feature = "webauthn")]
    async fn start_webauthn_registration_impl(
        &self,
        req: authn_pb::StartWebAuthnRegistrationRequest,
    ) -> Result<authn_pb::StartWebAuthnRegistrationResponse, Status> {
        use webauthn_rs::prelude::{CredentialID, Passkey};

        if req.user_id.trim().is_empty() {
            return Err(Self::webauthn_user_id_required_status());
        }
        let user = self
            .users
            .get_user_by_id(&req.user_id)
            .await
            .map_err(|err| {
                authn_internal_status("start_webauthn_registration_user_load", err.to_string())
            })?
            .ok_or_else(authn_user_not_found_status)?;
        if !req.tenant_id.trim().is_empty() && req.tenant_id != user.tenant_id {
            return Err(webauthn_user_tenant_mismatch_status(
                "start_webauthn_registration",
            ));
        }
        if !req.project_id.trim().is_empty() && req.project_id != user.project_id {
            return Err(webauthn_user_project_mismatch_status(
                "start_webauthn_registration",
            ));
        }

        let exclude_credentials = self
            .load_webauthn_passkeys(&user.user_id)
            .await?
            .iter()
            .map(|rec| {
                serde_json::from_str::<Passkey>(&rec.passkey_json)
                    .map(|passkey| passkey.cred_id().clone())
                    .map_err(|err| {
                        authn_internal_status(
                            "start_webauthn_registration_decode_passkey",
                            format!("decode WebAuthn passkey failed: {err}"),
                        )
                    })
            })
            .collect::<Result<Vec<CredentialID>, Status>>()?;
        let webauthn = self.webauthn()?;
        let user_uuid =
            Uuid::parse_str(&user.user_id).map_err(|_| Self::webauthn_user_id_uuid_status())?;
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
                authn_internal_status(
                    "start_webauthn_registration_begin",
                    format!("start WebAuthn registration failed: {err:?}"),
                )
            })?;
        let creation_json = serde_json::to_string(&creation).map_err(|err| {
            authn_internal_status(
                "start_webauthn_registration_challenge_serialize",
                format!("serialize WebAuthn registration challenge failed: {err}"),
            )
        })?;
        let challenge_b64 =
            webauthn_softauth::challenge_from_options(&creation_json).unwrap_or_default();
        let state_json = serde_json::to_string(&WebAuthnStateEnvelope {
            state,
            label: req.label,
            challenge: challenge_b64,
        })
        .map_err(|err| {
            authn_internal_status(
                "start_webauthn_registration_state_serialize",
                format!("serialize WebAuthn registration state failed: {err}"),
            )
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
        use webauthn_rs::prelude::{PasskeyRegistration, RegisterPublicKeyCredential, Url};

        Self::validate_webauthn_finish_fields(&req.challenge_id, &req.public_key_credential_json)?;
        let challenge = self
            .load_webauthn_challenge(&req.challenge_id, "registration")
            .await?;
        let mut user = self
            .users
            .get_user_by_id(&challenge.user_id)
            .await
            .map_err(|err| {
                authn_internal_status("finish_webauthn_registration_user_load", err.to_string())
            })?
            .ok_or_else(authn_user_not_found_status)?;
        if challenge.tenant_id != user.tenant_id || challenge.project_id != user.project_id {
            return Err(webauthn_invalid_ceremony_status(
                "finish_webauthn_registration",
            ));
        }
        let envelope: WebAuthnStateEnvelope<PasskeyRegistration> =
            serde_json::from_str(&challenge.state_json).map_err(|err| {
                authn_internal_status(
                    "finish_webauthn_registration_state_decode",
                    format!("decode WebAuthn registration state failed: {err}"),
                )
            })?;
        // Q#9-11: dev soft-authenticator path. The harness sends a sentinel; the
        // broker mints a REAL "none"-attestation credential (P-256/ES256) that the
        // production verifier below checks legitimately. Fail-closed off in prod.
        let credential: RegisterPublicKeyCredential = if webauthn_softauth::test_mode_enabled()
            && req.public_key_credential_json.trim() == webauthn_softauth::TEST_CREDENTIAL_SENTINEL
        {
            let cfg = self.webauthn_config()?;
            let origin = Url::parse(&cfg.rp_origin)
                .map_err(|err| {
                    authn_internal_status(
                        "finish_webauthn_registration_origin_parse",
                        format!("invalid WebAuthn origin: {err}"),
                    )
                })?
                .origin()
                .ascii_serialization();
            let json = webauthn_softauth::make_registration(
                &user.user_id,
                &cfg.rp_id,
                &origin,
                &envelope.challenge,
            )
            .map_err(|err| {
                authn_internal_status(
                    "finish_webauthn_registration_dev_make",
                    format!("dev WebAuthn registration failed: {err}"),
                )
            })?;
            serde_json::from_str(&json).map_err(|err| {
                authn_internal_status(
                    "finish_webauthn_registration_dev_decode",
                    format!("decode dev WebAuthn credential failed: {err}"),
                )
            })?
        } else {
            serde_json::from_str(&req.public_key_credential_json)
                .map_err(|err| Self::invalid_webauthn_registration_credential_json_status(err))?
        };
        let dev_mode = webauthn_softauth::test_mode_enabled()
            && req.public_key_credential_json.trim() == webauthn_softauth::TEST_CREDENTIAL_SENTINEL;
        let passkey = match self
            .webauthn()?
            .finish_passkey_registration(&credential, &envelope.state)
        {
            Ok(passkey) => passkey,
            Err(_) if dev_mode => {
                // Dev idempotency (Q#9-11): the deterministic dev authenticator
                // yields ONE credential per user, so re-registering the same user
                // is rejected by webauthn-rs's exclude-credentials guard. In test
                // mode, treat a re-register as idempotent success when the user
                // already holds that passkey (NOT a fail-open: prod never reaches
                // this branch, and a genuinely-absent passkey still errors).
                use webauthn_rs::prelude::Passkey;
                let existing = self
                    .load_webauthn_passkeys(&user.user_id)
                    .await?
                    .into_iter()
                    .find_map(|rec| serde_json::from_str::<Passkey>(&rec.passkey_json).ok());
                match existing {
                    Some(passkey) => {
                        let credential_id = Self::webauthn_credential_id_text(passkey.cred_id())?;
                        self.consume_webauthn_challenge(&user.tenant_id, &req.challenge_id)
                            .await
                            .ok();
                        return Ok(authn_pb::FinishWebAuthnRegistrationResponse {
                            registered: true,
                            credential_id,
                            user_id: user.user_id,
                        });
                    }
                    None => return Err(Status::unauthenticated("invalid credential")),
                }
            }
            Err(_) => return Err(Status::unauthenticated("invalid credential")),
        };
        // masterplan 4.1: enforce the per-tenant WebAuthn policy on the VERIFIED
        // registration credential (attestation conveyance / resident-key / UV flag),
        // fail-closed, before the passkey is persisted. Reads only self-reported
        // flags webauthn-rs already parsed; the attestation signature chain is the
        // documented OpenSSL follow-up (not done here).
        let policy = self.resolve_webauthn_policy(&user.tenant_id).await;
        enforce_registration_policy(&policy, &credential)?;
        let credential_id = Self::webauthn_credential_id_text(passkey.cred_id())?;
        let passkey_json = serde_json::to_string(&passkey).map_err(|err| {
            authn_internal_status(
                "finish_webauthn_registration_passkey_serialize",
                format!("serialize WebAuthn passkey failed: {err}"),
            )
        })?;
        let label = if req.label.trim().is_empty() {
            envelope.label.as_str()
        } else {
            req.label.as_str()
        };
        self.insert_webauthn_passkey(&user, &credential_id, passkey_json, label)
            .await?;
        self.consume_webauthn_challenge(&user.tenant_id, &req.challenge_id)
            .await?;
        user.mfa_enabled = true;
        user.updated_at_unix = now_unix();
        self.users.put_user(user.clone()).await.map_err(|err| {
            authn_internal_status("finish_webauthn_registration_user_store", err.to_string())
        })?;
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
            return Err(Self::webauthn_user_id_required_status());
        }
        let user = self
            .users
            .get_user_by_id(&req.user_id)
            .await
            .map_err(|err| {
                authn_internal_status("start_webauthn_authentication_user_load", err.to_string())
            })?
            .ok_or_else(authn_user_not_found_status)?;
        if !req.tenant_id.trim().is_empty() && req.tenant_id != user.tenant_id {
            return Err(webauthn_user_tenant_mismatch_status(
                "start_webauthn_authentication",
            ));
        }
        if !req.project_id.trim().is_empty() && req.project_id != user.project_id {
            return Err(webauthn_user_project_mismatch_status(
                "start_webauthn_authentication",
            ));
        }
        let passkeys = self
            .load_webauthn_passkeys(&user.user_id)
            .await?
            .into_iter()
            .map(|rec| {
                serde_json::from_str::<Passkey>(&rec.passkey_json).map_err(|err| {
                    authn_internal_status(
                        "start_webauthn_authentication_decode_passkey",
                        format!("decode WebAuthn passkey failed: {err}"),
                    )
                })
            })
            .collect::<Result<Vec<_>, Status>>()?;
        if passkeys.is_empty() {
            return Err(webauthn_passkeys_required_status());
        }
        let (request_options, state) = self
            .webauthn()?
            .start_passkey_authentication(&passkeys)
            .map_err(|err| {
                authn_internal_status(
                    "start_webauthn_authentication_begin",
                    format!("start WebAuthn authentication failed: {err:?}"),
                )
            })?;
        let request_json = serde_json::to_string(&request_options).map_err(|err| {
            authn_internal_status(
                "start_webauthn_authentication_challenge_serialize",
                format!("serialize WebAuthn authentication challenge failed: {err}"),
            )
        })?;
        let challenge_b64 =
            webauthn_softauth::challenge_from_options(&request_json).unwrap_or_default();
        let state_json = serde_json::to_string(&WebAuthnStateEnvelope {
            state,
            label: String::new(),
            challenge: challenge_b64,
        })
        .map_err(|err| {
            authn_internal_status(
                "start_webauthn_authentication_state_serialize",
                format!("serialize WebAuthn authentication state failed: {err}"),
            )
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
        use webauthn_rs::prelude::{Passkey, PasskeyAuthentication, PublicKeyCredential, Url};

        Self::validate_webauthn_finish_fields(&req.challenge_id, &req.public_key_credential_json)?;
        let challenge = self
            .load_webauthn_challenge(&req.challenge_id, "authentication")
            .await?;
        let user = self
            .users
            .get_user_by_id(&challenge.user_id)
            .await
            .map_err(|err| {
                authn_internal_status("finish_webauthn_authentication_user_load", err.to_string())
            })?
            .ok_or_else(authn_user_not_found_status)?;
        if challenge.tenant_id != user.tenant_id || challenge.project_id != user.project_id {
            return Err(webauthn_invalid_ceremony_status(
                "finish_webauthn_authentication",
            ));
        }
        let envelope: WebAuthnStateEnvelope<PasskeyAuthentication> =
            serde_json::from_str(&challenge.state_json).map_err(|err| {
                authn_internal_status(
                    "finish_webauthn_authentication_state_decode",
                    format!("decode WebAuthn authentication state failed: {err}"),
                )
            })?;
        // Q#9-11: dev soft-authenticator path (see finish_webauthn_registration_impl).
        // Signs a genuine ES256 assertion with the keypair minted at registration;
        // the production verifier below checks it. Fail-closed off in prod.
        let credential: PublicKeyCredential = if webauthn_softauth::test_mode_enabled()
            && req.public_key_credential_json.trim() == webauthn_softauth::TEST_CREDENTIAL_SENTINEL
        {
            let cfg = self.webauthn_config()?;
            let origin = Url::parse(&cfg.rp_origin)
                .map_err(|err| {
                    authn_internal_status(
                        "finish_webauthn_authentication_origin_parse",
                        format!("invalid WebAuthn origin: {err}"),
                    )
                })?
                .origin()
                .ascii_serialization();
            // The userHandle (user UUID bytes) is derived inside make_assertion to
            // match exactly what start_passkey_* bound as the passkey user handle.
            let json = webauthn_softauth::make_assertion(
                &user.user_id,
                &cfg.rp_id,
                &origin,
                &envelope.challenge,
            )
            .map_err(|err| {
                authn_internal_status(
                    "finish_webauthn_authentication_dev_make",
                    format!("dev WebAuthn assertion failed: {err}"),
                )
            })?;
            serde_json::from_str(&json).map_err(|err| {
                authn_internal_status(
                    "finish_webauthn_authentication_dev_decode",
                    format!("decode dev WebAuthn assertion failed: {err}"),
                )
            })?
        } else {
            serde_json::from_str(&req.public_key_credential_json)
                .map_err(|err| Self::invalid_webauthn_authentication_credential_json_status(err))?
        };
        let result = self
            .webauthn()?
            .finish_passkey_authentication(&credential, &envelope.state)
            .map_err(|_| Status::unauthenticated("invalid credential"))?;
        // Fail-closed UV floor: passkeys are UV-bound (webauthn-rs verifies with
        // UserVerificationPolicy::Required), so a UV=false assertion is rejected
        // outright and the per-tenant policy can only TIGHTEN, never loosen, this.
        if !result.user_verified() {
            return Err(Status::unauthenticated("invalid credential"));
        }
        // masterplan 4.1: enforce the per-tenant WebAuthn policy at assertion. UV is
        // the only authenticator-reported signal here; this makes the posture
        // explicit + tenant-tunable. Fail-closed.
        let policy = self.resolve_webauthn_policy(&user.tenant_id).await;
        enforce_assertion_policy(&policy, result.user_verified())?;
        let credential_id = Self::webauthn_credential_id_text(result.cred_id())?;
        let mut matched = None;
        for rec in self.load_webauthn_passkeys(&user.user_id).await? {
            let mut passkey: Passkey = serde_json::from_str(&rec.passkey_json).map_err(|err| {
                authn_internal_status(
                    "finish_webauthn_authentication_passkey_decode",
                    format!("decode WebAuthn passkey failed: {err}"),
                )
            })?;
            if passkey.cred_id() == result.cred_id() {
                passkey.update_credential(&result);
                matched = Some(passkey);
                break;
            }
        }
        let passkey = matched.ok_or_else(|| Status::unauthenticated("invalid credential"))?;
        let passkey_json = serde_json::to_string(&passkey).map_err(|err| {
            authn_internal_status(
                "finish_webauthn_authentication_passkey_serialize",
                format!("serialize WebAuthn passkey failed: {err}"),
            )
        })?;
        self.update_webauthn_passkey_after_auth(&user.tenant_id, &credential_id, passkey_json)
            .await?;
        self.consume_webauthn_challenge(&user.tenant_id, &req.challenge_id)
            .await?;
        let now = now_unix();
        // Block 1 (auth_fix.md, Decision E): resolve role-derived grants once,
        // thread into the session record and the JWT.
        let (scopes, roles) =
            self.resolve_effective_grants(&user.user_id, &user.tenant_id, &user.project_id);
        let (session_id, session_expires) = self
            .create_login_session(
                &user,
                "webauthn".to_string(),
                scopes.clone(),
                roles.clone(),
                String::new(),
                now,
            )
            .await?;
        let (access_token, access_exp) = self.issue_access_token(
            &user.user_id,
            &user.tenant_id,
            &user.project_id,
            &scopes,
            &roles,
            "",
            &session_id,
            "webauthn",
            account_kind_to_proto(user.account_kind),
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
            // The passkey path HAS the user record, so project its profile
            // attributes rather than returning the empty map every path used to.
            principal: Some(authn_principal_to_pb_with_attributes(
                &principal,
                expires_at,
                account_kind_to_proto(user.account_kind),
                &user.profile_attributes_json,
            )),
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
    // ── Typed service-account grants + mTLS certificate bindings
    //    (fix_plan Phases 2+3) — delegating to the grants module. ──
    async fn create_service_account_grant(
        &self,
        request: Request<authn_pb::CreateServiceAccountGrantRequest>,
    ) -> Result<Response<authn_pb::CreateServiceAccountGrantResponse>, Status> {
        let pool = self.require_pool()?.clone();
        super::grants::create_service_account_grant(self, &pool, request).await
    }
    async fn get_service_account_grant(
        &self,
        request: Request<authn_pb::GetServiceAccountGrantRequest>,
    ) -> Result<Response<authn_pb::GetServiceAccountGrantResponse>, Status> {
        let pool = self.require_pool()?.clone();
        super::grants::get_service_account_grant(self, &pool, request).await
    }
    async fn list_service_account_grants(
        &self,
        request: Request<authn_pb::ListServiceAccountGrantsRequest>,
    ) -> Result<Response<authn_pb::ListServiceAccountGrantsResponse>, Status> {
        let pool = self.require_pool()?.clone();
        super::grants::list_service_account_grants(self, &pool, request).await
    }
    async fn replace_service_account_grant(
        &self,
        request: Request<authn_pb::ReplaceServiceAccountGrantRequest>,
    ) -> Result<Response<authn_pb::ReplaceServiceAccountGrantResponse>, Status> {
        let pool = self.require_pool()?.clone();
        super::grants::replace_service_account_grant(self, &pool, request).await
    }
    async fn rotate_service_account_identity(
        &self,
        request: Request<authn_pb::RotateServiceAccountIdentityRequest>,
    ) -> Result<Response<authn_pb::RotateServiceAccountIdentityResponse>, Status> {
        let pool = self.require_pool()?.clone();
        super::grants::rotate_service_account_identity(self, &pool, request).await
    }
    async fn transfer_service_account_grant(
        &self,
        request: Request<authn_pb::TransferServiceAccountGrantRequest>,
    ) -> Result<Response<authn_pb::TransferServiceAccountGrantResponse>, Status> {
        let pool = self.require_pool()?.clone();
        super::grants::transfer_service_account_grant(self, &pool, request).await
    }
    async fn revoke_service_account_grant(
        &self,
        request: Request<authn_pb::RevokeServiceAccountGrantRequest>,
    ) -> Result<Response<authn_pb::RevokeServiceAccountGrantResponse>, Status> {
        let pool = self.require_pool()?.clone();
        super::grants::revoke_service_account_grant(self, &pool, request).await
    }
    async fn create_certificate_binding(
        &self,
        request: Request<authn_pb::CreateCertificateBindingRequest>,
    ) -> Result<Response<authn_pb::CreateCertificateBindingResponse>, Status> {
        let pool = self.require_pool()?.clone();
        super::grants::create_certificate_binding(self, &pool, request).await
    }
    async fn list_certificate_bindings(
        &self,
        request: Request<authn_pb::ListCertificateBindingsRequest>,
    ) -> Result<Response<authn_pb::ListCertificateBindingsResponse>, Status> {
        let pool = self.require_pool()?.clone();
        super::grants::list_certificate_bindings(self, &pool, request).await
    }
    async fn revoke_certificate_binding(
        &self,
        request: Request<authn_pb::RevokeCertificateBindingRequest>,
    ) -> Result<Response<authn_pb::RevokeCertificateBindingResponse>, Status> {
        let pool = self.require_pool()?.clone();
        super::grants::revoke_certificate_binding(self, &pool, request).await
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
            Err(webauthn_feature_required_status())
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
            Err(webauthn_feature_required_status())
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
            Err(webauthn_feature_required_status())
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
            Err(webauthn_feature_required_status())
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

#[cfg(test)]
mod authn_validation_tests {
    use super::*;
    use crate::proto::{ErrorDetail, ErrorKind};
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;

    fn decode_detail(status: &Status) -> ErrorDetail {
        let raw = status
            .metadata()
            .get_bin(ERROR_DETAIL_METADATA_KEY)
            .expect("typed detail trailer is present")
            .to_bytes()
            .expect("typed detail trailer decodes to bytes");
        crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
    }

    fn assert_single_field_violation(status: &Status, field: &str, description: &str) {
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, field);
        assert_eq!(detail.field_violations[0].description, description);
    }

    fn assert_capability_detail(
        status: &Status,
        operation: &str,
        capability_required: &str,
        message: &str,
    ) {
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Capability as i32);
        assert_eq!(detail.backend, "authn");
        assert_eq!(detail.operation, operation);
        assert_eq!(detail.capability_required, capability_required);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
    }

    fn assert_policy_detail(
        status: &Status,
        operation: &str,
        policy_decision_id: &str,
        message: &str,
    ) {
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Policy as i32);
        assert_eq!(detail.operation, operation);
        assert_eq!(detail.policy_decision_id, policy_decision_id);
        assert!(detail.backend.is_empty());
        assert!(detail.capability_required.is_empty());
        assert!(detail.field_violations.is_empty());
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
    }

    fn assert_permission_policy_detail(
        status: &Status,
        operation: &str,
        policy_decision_id: &str,
        message: &str,
    ) {
        assert_eq!(status.code(), tonic::Code::PermissionDenied);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Policy as i32);
        assert_eq!(detail.operation, operation);
        assert_eq!(detail.policy_decision_id, policy_decision_id);
        assert!(detail.backend.is_empty());
        assert!(detail.capability_required.is_empty());
        assert!(detail.field_violations.is_empty());
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
    }

    fn assert_schema_not_found_detail(
        status: &Status,
        operation: &str,
        schema_code: &str,
        message: &str,
    ) {
        assert_eq!(status.code(), tonic::Code::NotFound);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Schema as i32);
        assert_eq!(detail.backend, "authn");
        assert_eq!(detail.operation, operation);
        assert_eq!(detail.capability_required, schema_code);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
    }

    fn assert_internal_detail(status: &Status, operation: &str, message: &str) {
        assert_eq!(status.code(), tonic::Code::Internal);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Internal as i32);
        assert_eq!(detail.backend, "authn");
        assert_eq!(detail.operation, operation);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
        assert!(detail.field_violations.is_empty());
    }

    #[test]
    fn authn_internal_status_carries_typed_detail() {
        let status = authn_internal_status("user_is_active", "user store failed");
        assert_internal_detail(&status, "user_is_active", "user store failed");
    }

    #[test]
    fn authn_missing_runtime_capability_carries_typed_detail() {
        let svc = AuthnServiceImpl::new(
            authn::AuthnConfig::default(),
            crate::runtime::security::SecurityConfig::default(),
        );
        let err = match svc.authn_runtime() {
            Err(status) => status,
            Ok(_) => panic!("runtime-less authn service must fail closed"),
        };

        assert_capability_detail(
            &err,
            "native_entity_dispatch",
            "runtime_native_entity_dispatch",
            "this operation requires the native typed authn runtime",
        );
    }

    #[test]
    fn webauthn_feature_required_status_carries_capability_detail() {
        let err = webauthn_feature_required_status();

        assert_capability_detail(
            &err,
            "webauthn_rpc",
            "webauthn_feature",
            "WebAuthn requires building UDB with the `webauthn` feature",
        );
    }

    #[test]
    fn webauthn_config_statuses_carry_capability_detail() {
        let missing_rp = webauthn_config_capability_status(
            "webauthn_relying_party_config",
            "webauthn_rp_config",
            "WebAuthn requires UDB_WEBAUTHN_RP_ID and UDB_WEBAUTHN_ORIGIN",
        );
        assert_capability_detail(
            &missing_rp,
            "webauthn_relying_party_config",
            "webauthn_rp_config",
            "WebAuthn requires UDB_WEBAUTHN_RP_ID and UDB_WEBAUTHN_ORIGIN",
        );

        let insecure_origin = webauthn_config_capability_status(
            "webauthn_relying_party_config",
            "webauthn_https_origin",
            "production WebAuthn requires an https UDB_WEBAUTHN_ORIGIN",
        );
        assert_capability_detail(
            &insecure_origin,
            "webauthn_relying_party_config",
            "webauthn_https_origin",
            "production WebAuthn requires an https UDB_WEBAUTHN_ORIGIN",
        );

        let unsupported_policy = webauthn_config_capability_status(
            "webauthn_policy_config",
            "webauthn_policy_builder_support",
            "requested WebAuthn attestation/resident-key/user-verification policy \
             is not enforceable in this build (host-blocked on webauthn-rs 0.5 \
             builder support); refusing to silently ignore it in production",
        );
        assert_capability_detail(
            &unsupported_policy,
            "webauthn_policy_config",
            "webauthn_policy_builder_support",
            "requested WebAuthn attestation/resident-key/user-verification policy \
             is not enforceable in this build (host-blocked on webauthn-rs 0.5 \
             builder support); refusing to silently ignore it in production",
        );

        let invalid_origin = webauthn_config_capability_status(
            "webauthn_relying_party_config",
            "webauthn_origin_url",
            "invalid WebAuthn origin: relative URL without a base",
        );
        assert_capability_detail(
            &invalid_origin,
            "webauthn_relying_party_config",
            "webauthn_origin_url",
            "invalid WebAuthn origin: relative URL without a base",
        );
    }

    #[test]
    fn authn_oidc_and_passkey_preconditions_carry_typed_detail() {
        assert_policy_detail(
            &oidc_provider_disabled_status(),
            "oidc_authenticate",
            "identity_provider_disabled",
            "identity provider is disabled for this tenant",
        );

        assert_capability_detail(
            &oidc_jwks_url_invalid_status("relative URL without a base"),
            "oidc_provider_config",
            "oidc_jwks_url",
            "configured OIDC jwks_url is invalid: relative URL without a base",
        );

        assert_policy_detail(
            &webauthn_passkeys_required_status(),
            "webauthn_authentication",
            "webauthn_passkey_required",
            "user has no registered WebAuthn passkeys",
        );
    }

    #[test]
    fn authn_lookup_misses_carry_typed_schema_detail() {
        assert_schema_not_found_detail(
            &authn_user_not_found_status(),
            "user_lookup",
            "authn_user_not_found",
            "user not found",
        );

        assert_schema_not_found_detail(
            &authn_otp_not_found_status(),
            "otp_lookup",
            "authn_otp_not_found",
            "otp not found",
        );

        assert_schema_not_found_detail(
            &authn_device_not_found_status(),
            "device_revoke",
            "authn_device_not_found_or_already_revoked",
            "device not found or already revoked",
        );
    }

    #[test]
    fn authn_passkey_conflict_carries_schema_detail() {
        let err = authn_schema_already_exists_status(
            "store_webauthn_passkey",
            "webauthn_passkey_already_exists",
            "store WebAuthn passkey failed: duplicate key",
        );
        assert_eq!(err.code(), tonic::Code::AlreadyExists);
        assert_eq!(
            err.message(),
            "store WebAuthn passkey failed: duplicate key"
        );
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Schema as i32);
        assert_eq!(detail.backend, "authn");
        assert_eq!(detail.operation, "store_webauthn_passkey");
        assert_eq!(
            detail.capability_required,
            "webauthn_passkey_already_exists"
        );
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
    }

    #[test]
    fn native_authz_denial_carries_permission_policy_detail() {
        let status = native_authz_denied_status(
            "authn.create_user",
            "native.rpc:/udb.core.authn.services.v1.AuthnService/CreateUser",
            "authz_decision_123",
            "denied by policy p1",
        );

        assert_permission_policy_detail(
            &status,
            "authn_native_rpc_authorize",
            "authz_decision_123",
            "action 'authn.create_user' on 'native.rpc:/udb.core.authn.services.v1.AuthnService/CreateUser' denied by authz policy (denied by policy p1)",
        );
    }

    #[test]
    fn webauthn_permission_denials_carry_typed_policy_detail() {
        assert_permission_policy_detail(
            &webauthn_invalid_ceremony_status("finish_webauthn_registration"),
            "finish_webauthn_registration",
            "invalid_webauthn_ceremony",
            "invalid WebAuthn ceremony",
        );

        assert_permission_policy_detail(
            &webauthn_user_tenant_mismatch_status("start_webauthn_registration"),
            "start_webauthn_registration",
            "tenant_id_user_mismatch",
            "tenant_id does not match user",
        );

        assert_permission_policy_detail(
            &webauthn_user_project_mismatch_status("start_webauthn_authentication"),
            "start_webauthn_authentication",
            "project_id_user_mismatch",
            "project_id does not match user",
        );
    }

    #[test]
    fn require_uuid_arg_carries_field_violations() {
        let missing = AuthnServiceImpl::require_uuid_arg(" ", "otp_id")
            .expect_err("missing UUID argument must fail");
        assert_eq!(missing.code(), tonic::Code::InvalidArgument);
        assert_eq!(missing.message(), "otp_id is required");
        assert_single_field_violation(&missing, "otp_id", "must be a non-empty UUID");

        let malformed = AuthnServiceImpl::require_uuid_arg("not-a-uuid", "device_id")
            .expect_err("malformed UUID argument must fail");
        assert_eq!(malformed.code(), tonic::Code::InvalidArgument);
        assert_eq!(malformed.message(), "device_id must be a valid UUID");
        assert_single_field_violation(&malformed, "device_id", "must be a valid UUID");
    }

    #[test]
    fn webauthn_boundary_validation_carries_field_violations() {
        let missing_user = AuthnServiceImpl::webauthn_user_id_required_status();
        assert_eq!(missing_user.code(), tonic::Code::InvalidArgument);
        assert_eq!(missing_user.message(), "user_id is required");
        assert_single_field_violation(&missing_user, "user_id", "must be a non-empty user id");

        let bad_user_uuid = AuthnServiceImpl::webauthn_user_id_uuid_status();
        assert_eq!(bad_user_uuid.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            bad_user_uuid.message(),
            "WebAuthn users must have UUID user_id"
        );
        assert_single_field_violation(
            &bad_user_uuid,
            "user_id",
            "must be a valid UUID for WebAuthn ceremonies",
        );

        let bad_challenge = AuthnServiceImpl::webauthn_challenge_id_invalid_status();
        assert_eq!(bad_challenge.code(), tonic::Code::InvalidArgument);
        assert_eq!(bad_challenge.message(), "challenge_id must be a valid UUID");
        assert_single_field_violation(&bad_challenge, "challenge_id", "must be a valid UUID");

        let missing_finish = AuthnServiceImpl::validate_webauthn_finish_fields(" ", "")
            .expect_err("missing WebAuthn finish fields must fail");
        assert_eq!(missing_finish.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            missing_finish.message(),
            "challenge_id and public_key_credential_json are required"
        );
        let detail = decode_detail(&missing_finish);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 2);
        assert_eq!(detail.field_violations[0].field, "challenge_id");
        assert_eq!(
            detail.field_violations[0].description,
            "must be a non-empty WebAuthn challenge id"
        );
        assert_eq!(
            detail.field_violations[1].field,
            "public_key_credential_json"
        );
        assert_eq!(
            detail.field_violations[1].description,
            "must be a non-empty WebAuthn credential JSON payload"
        );

        let bad_registration =
            AuthnServiceImpl::invalid_webauthn_registration_credential_json_status(
                "expected value",
            );
        assert_eq!(bad_registration.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            bad_registration.message(),
            "invalid WebAuthn registration credential JSON: expected value"
        );
        assert_single_field_violation(
            &bad_registration,
            "public_key_credential_json",
            "must decode as a WebAuthn registration credential",
        );

        let bad_authentication =
            AuthnServiceImpl::invalid_webauthn_authentication_credential_json_status(
                "expected value",
            );
        assert_eq!(bad_authentication.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            bad_authentication.message(),
            "invalid WebAuthn authentication credential JSON: expected value"
        );
        assert_single_field_violation(
            &bad_authentication,
            "public_key_credential_json",
            "must decode as a WebAuthn authentication credential",
        );
    }

    #[test]
    fn oidc_boundary_validation_carries_field_violations() {
        let missing_token = AuthnServiceImpl::oidc_id_token_required_status();
        assert_eq!(missing_token.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            missing_token.message(),
            "OIDC authentication requires external_token or bearer_token containing an ID token"
        );
        let detail = decode_detail(&missing_token);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 2);
        assert_eq!(detail.field_violations[0].field, "external_token");
        assert_eq!(
            detail.field_violations[0].description,
            "must contain an OIDC ID token when bearer_token is empty"
        );
        assert_eq!(detail.field_violations[1].field, "bearer_token");
        assert_eq!(
            detail.field_violations[1].description,
            "must contain an OIDC ID token when external_token is empty"
        );

        let missing_issuer = AuthnServiceImpl::oidc_issuer_required_status();
        assert_eq!(missing_issuer.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            missing_issuer.message(),
            "OIDC issuer is required (provider registry, request issuer, or UDB_OIDC_ISSUER)"
        );
        assert_single_field_violation(
            &missing_issuer,
            "issuer",
            "must be supplied by the provider registry, request issuer, or UDB_OIDC_ISSUER",
        );

        let missing_client = AuthnServiceImpl::oidc_client_id_required_status();
        assert_eq!(missing_client.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            missing_client.message(),
            "OIDC client_id/audience is required"
        );
        let detail = decode_detail(&missing_client);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 2);
        assert_eq!(detail.field_violations[0].field, "client_id");
        assert_eq!(
            detail.field_violations[0].description,
            "must be supplied by the request, provider registry, or UDB_OIDC_CLIENT_ID"
        );
        assert_eq!(detail.field_violations[1].field, "audience");
        assert_eq!(
            detail.field_violations[1].description,
            "must be supplied when client_id is empty and no provider/default client is configured"
        );

        let mismatch = AuthnServiceImpl::oidc_client_id_audience_mismatch_status();
        assert_eq!(mismatch.code(), tonic::Code::InvalidArgument);
        assert_eq!(mismatch.message(), "OIDC client_id and audience must match");
        let detail = decode_detail(&mismatch);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 2);
        assert_eq!(detail.field_violations[0].field, "client_id");
        assert_eq!(
            detail.field_violations[0].description,
            "must match audience when both are supplied"
        );
        assert_eq!(detail.field_violations[1].field, "audience");
        assert_eq!(
            detail.field_violations[1].description,
            "must match client_id when both are supplied"
        );

        let missing_nonce = AuthnServiceImpl::oidc_nonce_required_status();
        assert_eq!(missing_nonce.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            missing_nonce.message(),
            "OIDC nonce is required in attributes[\"nonce\"]"
        );
        assert_single_field_violation(
            &missing_nonce,
            "attributes.nonce",
            "must include a non-empty OIDC nonce",
        );

        let bad_issuer = AuthnServiceImpl::invalid_oidc_issuer_status("relative URL");
        assert_eq!(bad_issuer.code(), tonic::Code::InvalidArgument);
        assert_eq!(bad_issuer.message(), "invalid OIDC issuer: relative URL");
        assert_single_field_violation(&bad_issuer, "issuer", "must be a valid OIDC issuer URL");
    }
}

// ── Per-tenant WebAuthn policy enforcement (masterplan 4.1) ───────────────────
// Drives the EXACT enforcement functions the serve path (`finish_webauthn_*_impl`)
// invokes, against a REAL credential minted by the dev soft-authenticator harness
// (`webauthn_softauth`). Proves a non-conformant authenticator is rejected
// (FailedPrecondition) and a conformant one passes — no DB / network required.
#[cfg(all(test, feature = "webauthn"))]
mod webauthn_policy_tests {
    use super::*;
    use crate::proto::{ErrorDetail, ErrorKind};
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;
    use webauthn_rs::prelude::RegisterPublicKeyCredential;

    fn assert_attestation_field_violation(status: &Status, field: &str, description: &str) {
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        let raw = status
            .metadata()
            .get_bin(ERROR_DETAIL_METADATA_KEY)
            .expect("typed detail trailer is present")
            .to_bytes()
            .expect("typed detail trailer decodes to bytes");
        let detail = crate::runtime::executor_utils::decode_error_detail_from_raw(&raw);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, field);
        assert_eq!(detail.field_violations[0].description, description);
        assert!(!detail.retryable);
    }

    fn assert_attestation_capability_detail(
        status: &Status,
        operation: &str,
        capability_required: &str,
        message: &str,
    ) {
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert_eq!(status.message(), message);
        let raw = status
            .metadata()
            .get_bin(ERROR_DETAIL_METADATA_KEY)
            .expect("typed detail trailer is present")
            .to_bytes()
            .expect("typed detail trailer decodes to bytes");
        let detail = crate::runtime::executor_utils::decode_error_detail_from_raw(&raw);
        assert_eq!(detail.kind, ErrorKind::Capability as i32);
        assert_eq!(detail.backend, "authn");
        assert_eq!(detail.operation, operation);
        assert_eq!(detail.capability_required, capability_required);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
    }

    fn assert_webauthn_policy_detail(
        status: &Status,
        operation: &str,
        policy_decision_id: &str,
        message: &str,
    ) {
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert_eq!(status.message(), message);
        let raw = status
            .metadata()
            .get_bin(ERROR_DETAIL_METADATA_KEY)
            .expect("typed detail trailer is present")
            .to_bytes()
            .expect("typed detail trailer decodes to bytes");
        let detail = crate::runtime::executor_utils::decode_error_detail_from_raw(&raw);
        assert_eq!(detail.kind, ErrorKind::Policy as i32);
        assert_eq!(detail.operation, operation);
        assert_eq!(detail.policy_decision_id, policy_decision_id);
        assert!(detail.backend.is_empty());
        assert!(detail.capability_required.is_empty());
        assert!(detail.field_violations.is_empty());
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
    }

    // A genuine "none"-attestation registration credential (UV flag set, no
    // credProps.rk), exactly as the soft-authenticator feeds the serve path.
    fn soft_registration_credential() -> RegisterPublicKeyCredential {
        let user = uuid::Uuid::new_v4().to_string();
        let json = webauthn_softauth::make_registration(
            &user,
            "localhost",
            "http://localhost",
            "Y2hhbGxlbmdl", // any b64url challenge; conveyance/UV/rk gating ignores it
        )
        .expect("soft registration credential");
        serde_json::from_str(&json).expect("parse RegisterPublicKeyCredential")
    }

    #[test]
    fn attestation_crypto_failures_carry_capability_detail() {
        let err = webauthn_attestation_crypto_status(
            "WebAuthn policy: create packed attestation verifier failed: unavailable",
        );
        assert_attestation_capability_detail(
            &err,
            "webauthn_attestation_crypto",
            "webauthn_attestation_crypto",
            "WebAuthn policy: create packed attestation verifier failed: unavailable",
        );
    }

    #[test]
    fn attestation_untrusted_chain_carries_policy_detail() {
        let err = webauthn_attestation_chain_untrusted_status();
        assert_webauthn_policy_detail(
            &err,
            "webauthn_attestation_trust",
            "webauthn_attestation_chain_not_trusted",
            "WebAuthn policy: attestation certificate chain is not trusted",
        );
    }

    #[test]
    fn parses_soft_authenticator_attestation_object() {
        let cred = soft_registration_credential();
        let attestation =
            parse_attestation_object(cred.response.attestation_object.as_ref()).expect("parse");
        assert_eq!(attestation.fmt, "none");
        assert!(attestation.x5c_der.is_empty());
        // Soft authenticator sets UP|UV|AT at registration → UV flag must read true.
        assert!(authenticator_data_user_verified(&attestation.auth_data));
    }

    #[test]
    fn registration_policy_unparseable_attestation_object_carries_field_violation() {
        let policy = WebAuthnPolicy::default();
        let mut cred = soft_registration_credential();
        cred.response.attestation_object = vec![0xff, 0x00].into();

        let err = enforce_registration_policy(&policy, &cred)
            .expect_err("unparseable attestationObject must fail before policy checks");
        assert_eq!(
            err.message(),
            "WebAuthn policy: unparseable attestationObject (cannot evaluate conveyance/UV)"
        );
        assert_attestation_field_violation(
            &err,
            "attestationObject",
            "must decode as a WebAuthn attestationObject CBOR map",
        );
    }

    #[test]
    fn registration_policy_malformed_auth_data_carries_field_violation() {
        let policy = WebAuthnPolicy::default();
        let mut cred = soft_registration_credential();
        let mut attestation_object = Vec::new();
        attestation_object.push(0xa3); // map(3)
        attestation_object.extend_from_slice(&[0x63, b'f', b'm', b't']);
        attestation_object.extend_from_slice(&[0x64, b'n', b'o', b'n', b'e']);
        attestation_object.extend_from_slice(&[0x67, b'a', b't', b't', b'S', b't', b'm', b't']);
        attestation_object.push(0xa0); // empty attStmt map
        attestation_object
            .extend_from_slice(&[0x68, b'a', b'u', b't', b'h', b'D', b'a', b't', b'a']);
        attestation_object.extend_from_slice(&[0x43, 0x01, 0x02, 0x03]); // bytes(3)
        cred.response.attestation_object = attestation_object.into();

        let err = enforce_registration_policy(&policy, &cred)
            .expect_err("short authData must fail before policy checks");
        assert_eq!(
            err.message(),
            "WebAuthn policy: malformed authenticator data (cannot evaluate UV)"
        );
        assert_attestation_field_violation(
            &err,
            "authData",
            "must be at least 37 bytes to evaluate WebAuthn user verification",
        );
    }

    #[test]
    fn parses_attestation_x5c_from_att_stmt() {
        let mut attestation_object = Vec::new();
        attestation_object.push(0xa3); // map(3)
        attestation_object.extend_from_slice(&[0x63, b'f', b'm', b't']);
        attestation_object.extend_from_slice(&[0x66, b'p', b'a', b'c', b'k', b'e', b'd']);
        attestation_object.extend_from_slice(&[0x67, b'a', b't', b't', b'S', b't', b'm', b't']);
        attestation_object.push(0xa1); // attStmt map(1)
        attestation_object.extend_from_slice(&[0x63, b'x', b'5', b'c']);
        attestation_object.push(0x81); // array(1)
        attestation_object.extend_from_slice(&[0x43, 0x30, 0x82, 0x01]); // bytes(3)
        attestation_object
            .extend_from_slice(&[0x68, b'a', b'u', b't', b'h', b'D', b'a', b't', b'a']);
        attestation_object.extend_from_slice(&[0x58, 0x25]); // bytes(37)
        attestation_object.extend_from_slice(&[0u8; 37]);

        let attestation = parse_attestation_object(&attestation_object).expect("parse x5c");
        assert_eq!(attestation.fmt, "packed");
        assert_eq!(attestation.x5c_der, vec![vec![0x30, 0x82, 0x01]]);
        assert_eq!(attestation.auth_data.len(), 37);
    }

    #[test]
    fn parses_packed_attestation_statement_signature_fields() {
        let mut attestation_object = Vec::new();
        attestation_object.push(0xa3); // map(3)
        attestation_object.extend_from_slice(&[0x63, b'f', b'm', b't']);
        attestation_object.extend_from_slice(&[0x66, b'p', b'a', b'c', b'k', b'e', b'd']);
        attestation_object.extend_from_slice(&[0x67, b'a', b't', b't', b'S', b't', b'm', b't']);
        attestation_object.push(0xa3); // attStmt map(3)
        attestation_object.extend_from_slice(&[0x63, b'a', b'l', b'g']);
        attestation_object.push(0x26); // -7 (ES256)
        attestation_object.extend_from_slice(&[0x63, b's', b'i', b'g']);
        attestation_object.extend_from_slice(&[0x43, 0x01, 0x02, 0x03]); // bytes(3)
        attestation_object.extend_from_slice(&[0x63, b'x', b'5', b'c']);
        attestation_object.push(0x81); // array(1)
        attestation_object.extend_from_slice(&[0x43, 0x30, 0x82, 0x01]); // bytes(3)
        attestation_object
            .extend_from_slice(&[0x68, b'a', b'u', b't', b'h', b'D', b'a', b't', b'a']);
        attestation_object.extend_from_slice(&[0x58, 0x25]); // bytes(37)
        attestation_object.extend_from_slice(&[0u8; 37]);

        let attestation =
            parse_attestation_object(&attestation_object).expect("parse packed attStmt");
        assert_eq!(attestation.fmt, "packed");
        assert_eq!(attestation.alg, Some(-7));
        assert_eq!(attestation.sig, Some(vec![1, 2, 3]));
        assert_eq!(attestation.x5c_der, vec![vec![0x30, 0x82, 0x01]]);
    }

    #[test]
    fn parses_tpm_attestation_statement_fields() {
        let mut attestation_object = Vec::new();
        attestation_object.push(0xa3); // map(3)
        attestation_object.extend_from_slice(&[0x63, b'f', b'm', b't']);
        attestation_object.extend_from_slice(&[0x63, b't', b'p', b'm']);
        attestation_object.extend_from_slice(&[0x67, b'a', b't', b't', b'S', b't', b'm', b't']);
        attestation_object.push(0xa6); // attStmt map(6)
        attestation_object.extend_from_slice(&[0x63, b'v', b'e', b'r']);
        attestation_object.extend_from_slice(&[0x63, b'2', b'.', b'0']);
        attestation_object.extend_from_slice(&[0x63, b'a', b'l', b'g']);
        attestation_object.extend_from_slice(&[0x39, 0x01, 0x00]); // -257 (RS256)
        attestation_object.extend_from_slice(&[0x63, b's', b'i', b'g']);
        attestation_object.extend_from_slice(&[0x43, 0x01, 0x02, 0x03]); // bytes(3)
        attestation_object
            .extend_from_slice(&[0x68, b'c', b'e', b'r', b't', b'I', b'n', b'f', b'o']);
        attestation_object.extend_from_slice(&[0x42, 0x04, 0x05]); // bytes(2)
        attestation_object.extend_from_slice(&[0x67, b'p', b'u', b'b', b'A', b'r', b'e', b'a']);
        attestation_object.extend_from_slice(&[0x42, 0x06, 0x07]); // bytes(2)
        attestation_object.extend_from_slice(&[0x63, b'x', b'5', b'c']);
        attestation_object.push(0x81); // array(1)
        attestation_object.extend_from_slice(&[0x43, 0x30, 0x82, 0x01]); // bytes(3)
        attestation_object
            .extend_from_slice(&[0x68, b'a', b'u', b't', b'h', b'D', b'a', b't', b'a']);
        attestation_object.extend_from_slice(&[0x58, 0x25]); // bytes(37)
        attestation_object.extend_from_slice(&[0u8; 37]);

        let attestation = parse_attestation_object(&attestation_object).expect("parse tpm attStmt");
        assert_eq!(attestation.fmt, "tpm");
        assert_eq!(attestation.ver.as_deref(), Some("2.0"));
        assert_eq!(attestation.alg, Some(-257));
        assert_eq!(attestation.sig, Some(vec![1, 2, 3]));
        assert_eq!(attestation.cert_info, Some(vec![4, 5]));
        assert_eq!(attestation.pub_area, Some(vec![6, 7]));
        assert_eq!(attestation.x5c_der, vec![vec![0x30, 0x82, 0x01]]);
    }

    fn fido_u2f_auth_data_with_ec2_key() -> Vec<u8> {
        let mut auth_data = Vec::new();
        auth_data.extend_from_slice(&[0xA5; 32]); // rpIdHash
        auth_data.push(0x41); // UP + AT
        auth_data.extend_from_slice(&[0, 0, 0, 7]); // signCount
        auth_data.extend_from_slice(&[0x00; 16]); // AAGUID
        auth_data.extend_from_slice(&3u16.to_be_bytes());
        auth_data.extend_from_slice(&[0x10, 0x11, 0x12]); // credentialId
        auth_data.push(0xA5); // COSE_Key map(5)
        auth_data.extend_from_slice(&[0x01, 0x02]); // 1: kty EC2
        auth_data.extend_from_slice(&[0x03, 0x26]); // 3: alg ES256 (-7)
        auth_data.extend_from_slice(&[0x20, 0x01]); // -1: crv P-256
        auth_data.push(0x21); // -2: x
        auth_data.extend_from_slice(&[0x58, 0x20]);
        auth_data.extend_from_slice(&[0x22; 32]);
        auth_data.push(0x22); // -3: y
        auth_data.extend_from_slice(&[0x58, 0x20]);
        auth_data.extend_from_slice(&[0x33; 32]);
        auth_data
    }

    #[test]
    fn fido_u2f_attestation_verification_data_matches_spec_order() {
        use sha2::{Digest, Sha256};

        let attestation = ParsedAttestationObject {
            fmt: "fido-u2f".to_string(),
            auth_data: fido_u2f_auth_data_with_ec2_key(),
            ver: None,
            alg: None,
            sig: Some(vec![1, 2, 3]),
            x5c_der: vec![vec![0x30, 0x82, 0x01]],
            cert_info: None,
            pub_area: None,
        };
        let client_data_json = br#"{"challenge":"abc"}"#;

        let data = fido_u2f_attestation_verification_data(&attestation, client_data_json)
            .expect("fido-u2f verification data");
        assert_eq!(data.len(), 1 + 32 + 32 + 3 + 65);
        assert_eq!(data[0], 0x00);
        assert_eq!(&data[1..33], &[0xA5; 32]);
        assert_eq!(&data[33..65], Sha256::digest(client_data_json).as_slice());
        assert_eq!(&data[65..68], &[0x10, 0x11, 0x12]);
        assert_eq!(data[68], 0x04);
        assert_eq!(&data[69..101], &[0x22; 32]);
        assert_eq!(&data[101..133], &[0x33; 32]);
    }

    #[test]
    fn android_key_attestation_signed_data_matches_spec_order() {
        use sha2::{Digest, Sha256};

        let attestation = ParsedAttestationObject {
            fmt: "android-key".to_string(),
            auth_data: vec![0xA7; 37],
            ver: None,
            alg: Some(-7),
            sig: Some(vec![1, 2, 3]),
            x5c_der: vec![vec![0x30, 0x82, 0x01]],
            cert_info: None,
            pub_area: None,
        };
        let client_data_json = br#"{"challenge":"android"}"#;

        let data = webauthn_basic_attestation_signed_data(&attestation, client_data_json);
        assert_eq!(data.len(), 37 + 32);
        assert_eq!(&data[..37], &[0xA7; 37]);
        assert_eq!(&data[37..], Sha256::digest(client_data_json).as_slice());
    }

    fn tpm_test_pub_area() -> Vec<u8> {
        vec![
            0x00, 0x01, // type: TPM_ALG_RSA
            0x00, 0x0b, // nameAlg: TPM_ALG_SHA256
            0x00, 0x00, 0x00, 0x00, // objectAttributes
            0x00, 0x00, // authPolicy: empty TPM2B_DIGEST
        ]
    }

    fn tpm_test_cert_info(extra_data: &[u8], attested_name: &[u8]) -> Vec<u8> {
        let mut cert_info = Vec::new();
        cert_info.extend_from_slice(&0xff54_4347u32.to_be_bytes());
        cert_info.extend_from_slice(&0x8017u16.to_be_bytes());
        cert_info.extend_from_slice(&0u16.to_be_bytes()); // qualifiedSigner
        cert_info.extend_from_slice(&(extra_data.len() as u16).to_be_bytes());
        cert_info.extend_from_slice(extra_data);
        cert_info.extend_from_slice(&[0u8; 17]); // clockInfo
        cert_info.extend_from_slice(&0u64.to_be_bytes()); // firmwareVersion
        cert_info.extend_from_slice(&(attested_name.len() as u16).to_be_bytes());
        cert_info.extend_from_slice(attested_name);
        cert_info.extend_from_slice(&0u16.to_be_bytes()); // qualifiedName
        cert_info
    }

    fn test_self_signed_x509_der() -> Vec<u8> {
        use openssl::asn1::Asn1Time;
        use openssl::bn::{BigNum, MsbOption};
        use openssl::hash::MessageDigest;
        use openssl::pkey::PKey;
        use openssl::rsa::Rsa;
        use openssl::x509::{X509, X509NameBuilder};

        let rsa = Rsa::generate(2048).expect("test RSA key");
        let key = PKey::from_rsa(rsa).expect("test PKey");
        let mut name = X509NameBuilder::new().expect("test X509 name builder");
        name.append_entry_by_text("CN", "udb-test")
            .expect("test X509 common name");
        let name = name.build();

        let mut serial = BigNum::new().expect("test serial number");
        serial
            .rand(128, MsbOption::MAYBE_ZERO, false)
            .expect("test serial entropy");
        let serial = serial.to_asn1_integer().expect("test ASN.1 serial");
        let not_before = Asn1Time::days_from_now(0).expect("test notBefore");
        let not_after = Asn1Time::days_from_now(1).expect("test notAfter");

        let mut cert = X509::builder().expect("test X509 builder");
        cert.set_version(2).expect("test X509 version");
        cert.set_serial_number(&serial)
            .expect("test X509 serial number");
        cert.set_subject_name(&name).expect("test X509 subject");
        cert.set_issuer_name(&name).expect("test X509 issuer");
        cert.set_pubkey(&key).expect("test X509 public key");
        cert.set_not_before(&not_before)
            .expect("test X509 notBefore");
        cert.set_not_after(&not_after).expect("test X509 notAfter");
        cert.sign(&key, MessageDigest::sha256())
            .expect("test X509 signature");
        cert.build().to_der().expect("test X509 DER")
    }

    #[test]
    fn tpm_cert_info_binds_extra_data_and_pub_area_name() {
        let client_data_json = br#"{"challenge":"tpm"}"#;
        let attestation = ParsedAttestationObject {
            fmt: "tpm".to_string(),
            auth_data: vec![0xA9; 37],
            ver: Some("2.0".to_string()),
            alg: Some(-257),
            sig: Some(vec![1, 2, 3]),
            x5c_der: vec![vec![0x30, 0x82, 0x01]],
            cert_info: None,
            pub_area: None,
        };
        let extra_data =
            tpm_attestation_extra_data(&attestation, client_data_json, -257).expect("extraData");
        let pub_area = tpm_test_pub_area();
        let pub_area_name = tpm_public_area_name(&pub_area).expect("pubArea name");
        let cert_info = tpm_test_cert_info(&extra_data, &pub_area_name);

        let parsed =
            tpm_parse_certify_info(&cert_info, &extra_data).expect("TPMS_CERTIFY_INFO binding");
        assert_eq!(parsed.attested_name, pub_area_name);

        let mut wrong_extra = extra_data.clone();
        wrong_extra[0] ^= 0xff;
        let err = tpm_parse_certify_info(&cert_info, &wrong_extra)
            .expect_err("mismatched TPM extraData must be rejected");
        assert_eq!(
            err.message(),
            "WebAuthn policy: tpm certInfo extraData does not match authenticator/client data"
        );
        assert_attestation_field_violation(
            &err,
            "attStmt.certInfo",
            "must bind to the authenticator data and clientDataHash",
        );

        let truncated = tpm_public_area_name(&[0x00, 0x01])
            .expect_err("truncated TPM pubArea must be rejected");
        assert_eq!(
            truncated.message(),
            "WebAuthn policy: tpm pubArea is truncated before nameAlg"
        );
        assert_attestation_field_violation(
            &truncated,
            "attStmt.pubArea",
            "must include a TPM public area nameAlg",
        );

        let unsupported_name_alg = tpm_name_digest_for_alg(0x0010)
            .err()
            .expect("unsupported TPM nameAlg must be rejected");
        assert_eq!(
            unsupported_name_alg.message(),
            "WebAuthn policy: tpm pubArea nameAlg 0x0010 is not supported"
        );
        assert_attestation_field_violation(
            &unsupported_name_alg,
            "attStmt.pubArea",
            "must use SHA-256, SHA-384, or SHA-512 as the TPM nameAlg",
        );
    }

    #[test]
    fn tpm_cert_info_rejects_malformed_structure_with_field_detail() {
        let client_data_json = br#"{"challenge":"tpm"}"#;
        let attestation = ParsedAttestationObject {
            fmt: "tpm".to_string(),
            auth_data: vec![0xA9; 37],
            ver: Some("2.0".to_string()),
            alg: Some(-257),
            sig: Some(vec![1, 2, 3]),
            x5c_der: vec![vec![0x30, 0x82, 0x01]],
            cert_info: None,
            pub_area: None,
        };
        let extra_data =
            tpm_attestation_extra_data(&attestation, client_data_json, -257).expect("extraData");
        let pub_area = tpm_test_pub_area();
        let pub_area_name = tpm_public_area_name(&pub_area).expect("pubArea name");
        let cert_info = tpm_test_cert_info(&extra_data, &pub_area_name);

        let mut bad_magic = cert_info.clone();
        bad_magic[0] ^= 0xff;
        let err = tpm_parse_certify_info(&bad_magic, &extra_data)
            .expect_err("invalid TPM certInfo magic must be rejected");
        assert_eq!(
            err.message(),
            "WebAuthn policy: tpm certInfo magic is invalid"
        );
        assert_attestation_field_violation(
            &err,
            "attStmt.certInfo",
            "must contain a valid TPM2B certInfo structure",
        );

        let mut bad_type = cert_info.clone();
        bad_type[5] ^= 0xff;
        let err = tpm_parse_certify_info(&bad_type, &extra_data)
            .expect_err("invalid TPM certInfo attestation type must be rejected");
        assert_eq!(
            err.message(),
            "WebAuthn policy: tpm certInfo type is not TPM_ST_ATTEST_CERTIFY"
        );
        assert_attestation_field_violation(
            &err,
            "attStmt.certInfo",
            "must contain a TPM_ST_ATTEST_CERTIFY attestation type",
        );

        let cert_info_prefix_len = 4 + 2 + 2 + 2 + extra_data.len();
        let truncated = &cert_info[..cert_info_prefix_len + 24];
        let err = tpm_parse_certify_info(truncated, &extra_data)
            .expect_err("truncated TPM certInfo body must be rejected");
        assert_eq!(
            err.message(),
            "WebAuthn policy: tpm certInfo truncated before certify info"
        );
        assert_attestation_field_violation(
            &err,
            "attStmt.certInfo",
            "must include TPM clockInfo, firmwareVersion, and certify info",
        );
    }

    #[test]
    fn demanded_attestation_without_x5c_fails_closed() {
        let err = verify_attestation_certificate_chain("packed", &[])
            .expect_err("attested formats without x5c must be rejected");
        assert_eq!(
            err.message(),
            "WebAuthn policy: attestation format 'packed' did not include attStmt.x5c"
        );
        assert_attestation_field_violation(
            &err,
            "attStmt.x5c",
            "must include at least one X.509 certificate for attestation chain validation",
        );
    }

    #[test]
    fn unsupported_attestation_chain_format_carries_field_violation() {
        let err = verify_attestation_certificate_chain("enterprise", &[vec![0x30]])
            .expect_err("unsupported attestation chain format must be rejected");
        assert_eq!(
            err.message(),
            "WebAuthn policy: attestation format 'enterprise' is not supported for OpenSSL chain validation"
        );
        assert_attestation_field_violation(
            &err,
            "fmt",
            "must be packed, tpm, android-key, or fido-u2f for attestation chain validation",
        );
    }

    #[test]
    fn malformed_attestation_chain_leaf_carries_field_violation() {
        let err = openssl_verify_x509_chain(&[vec![0x30, 0x82, 0x01]], Vec::new())
            .expect_err("malformed attestation chain leaf must be rejected");
        assert!(
            err.message()
                .starts_with("WebAuthn policy: parse attestation leaf certificate failed:"),
            "unexpected error: {}",
            err.message()
        );
        assert_attestation_field_violation(
            &err,
            "attStmt.x5c",
            "must contain a valid DER-encoded attestation leaf certificate",
        );
    }

    #[test]
    fn malformed_attestation_chain_intermediate_carries_field_violation() {
        let err = openssl_verify_x509_chain(
            &[test_self_signed_x509_der(), vec![0x30, 0x82, 0x01]],
            Vec::new(),
        )
        .expect_err("malformed attestation chain intermediate must be rejected");
        assert!(
            err.message()
                .starts_with("WebAuthn policy: parse attestation intermediate certificate failed:"),
            "unexpected error: {}",
            err.message()
        );
        assert_attestation_field_violation(
            &err,
            "attStmt.x5c",
            "must contain valid DER-encoded attestation intermediate certificates",
        );
    }

    #[test]
    fn demanded_attestation_without_trust_roots_carries_capability_detail() {
        let err = webauthn_attestation_roots_status(
            "WebAuthn policy: attestation format 'packed' requires configured trust roots \
             (UDB_WEBAUTHN_ATTESTATION_ROOTS_PEM or UDB_WEBAUTHN_ATTESTATION_ROOTS_PEM_PATH)",
        );

        assert_attestation_capability_detail(
            &err,
            "webauthn_attestation_trust",
            "webauthn_attestation_roots",
            "WebAuthn policy: attestation format 'packed' requires configured trust roots \
             (UDB_WEBAUTHN_ATTESTATION_ROOTS_PEM or UDB_WEBAUTHN_ATTESTATION_ROOTS_PEM_PATH)",
        );
    }

    #[test]
    fn attestation_roots_setup_failures_carry_capability_detail() {
        let read = webauthn_attestation_roots_status(
            "WebAuthn policy: read attestation roots PEM failed from missing.pem: access denied",
        );
        assert_attestation_capability_detail(
            &read,
            "webauthn_attestation_trust",
            "webauthn_attestation_roots",
            "WebAuthn policy: read attestation roots PEM failed from missing.pem: access denied",
        );

        let parse = webauthn_attestation_roots_status(
            "WebAuthn policy: parse attestation roots PEM failed: bad base64",
        );
        assert_attestation_capability_detail(
            &parse,
            "webauthn_attestation_trust",
            "webauthn_attestation_roots",
            "WebAuthn policy: parse attestation roots PEM failed: bad base64",
        );
    }

    #[test]
    fn packed_attestation_without_statement_signature_fails_closed() {
        let attestation = ParsedAttestationObject {
            fmt: "packed".to_string(),
            auth_data: vec![0u8; 37],
            ver: None,
            alg: Some(-7),
            sig: None,
            x5c_der: Vec::new(),
            cert_info: None,
            pub_area: None,
        };
        let err = verify_packed_attestation_signature(&attestation, b"{}")
            .expect_err("packed attestation without attStmt.sig must be rejected");
        assert_eq!(
            err.message(),
            "WebAuthn policy: packed attestation missing attStmt.sig"
        );
        assert_attestation_field_violation(
            &err,
            "attStmt.sig",
            "must be present for packed attestation signature verification",
        );
    }

    #[test]
    fn packed_attestation_malformed_leaf_certificate_carries_field_violation() {
        let attestation = ParsedAttestationObject {
            fmt: "packed".to_string(),
            auth_data: vec![0u8; 37],
            ver: None,
            alg: Some(-7),
            sig: Some(vec![1, 2, 3]),
            x5c_der: vec![vec![0x30, 0x82, 0x01]],
            cert_info: None,
            pub_area: None,
        };
        let err = verify_packed_attestation_signature(&attestation, b"{}")
            .expect_err("malformed packed attStmt.x5c must be rejected");
        assert!(
            err.message()
                .starts_with("WebAuthn policy: parse packed attestation leaf certificate failed:")
        );
        assert_attestation_field_violation(
            &err,
            "attStmt.x5c",
            "must contain a valid DER-encoded packed attestation leaf certificate",
        );
    }

    #[test]
    fn packed_attestation_invalid_signature_carries_field_violation() {
        let attestation = ParsedAttestationObject {
            fmt: "packed".to_string(),
            auth_data: vec![0u8; 37],
            ver: None,
            alg: Some(-257),
            sig: Some(vec![0u8; 256]),
            x5c_der: vec![test_self_signed_x509_der()],
            cert_info: None,
            pub_area: None,
        };
        let err = verify_packed_attestation_signature(&attestation, b"{}")
            .expect_err("invalid packed attStmt.sig must be rejected");
        assert_eq!(
            err.message(),
            "WebAuthn policy: packed attestation signature is invalid"
        );
        assert_attestation_field_violation(
            &err,
            "attStmt.sig",
            "must verify the packed attestation statement signature",
        );
    }

    #[test]
    fn packed_attestation_malformed_signature_carries_field_violation() {
        let attestation = ParsedAttestationObject {
            fmt: "packed".to_string(),
            auth_data: vec![0u8; 37],
            ver: None,
            alg: Some(-257),
            sig: Some(Vec::new()),
            x5c_der: vec![test_self_signed_x509_der()],
            cert_info: None,
            pub_area: None,
        };
        let err = verify_packed_attestation_signature(&attestation, b"{}")
            .expect_err("malformed packed attStmt.sig must be rejected");
        assert!(
            err.message()
                .starts_with("WebAuthn policy: verify packed attestation signature failed:"),
            "unexpected error: {}",
            err.message()
        );
        assert_attestation_field_violation(
            &err,
            "attStmt.sig",
            "must be a well-formed packed attestation statement signature",
        );
    }

    #[test]
    fn unsupported_attestation_alg_carries_field_violation() {
        let err = attestation_digest_for_alg("packed", -999)
            .err()
            .expect("unsupported attestation alg must be rejected");
        assert_eq!(
            err.message(),
            "WebAuthn policy: packed attestation alg -999 is not supported"
        );
        assert_attestation_field_violation(
            &err,
            "attStmt.alg",
            "must be a supported COSE algorithm for WebAuthn attestation verification",
        );
    }

    #[test]
    fn unsupported_attestation_format_carries_field_violation() {
        let attestation = ParsedAttestationObject {
            fmt: "enterprise".to_string(),
            auth_data: vec![0u8; 37],
            ver: None,
            alg: Some(-7),
            sig: Some(vec![1, 2, 3]),
            x5c_der: Vec::new(),
            cert_info: None,
            pub_area: None,
        };
        let err = verify_attestation_statement_signature("enterprise", &attestation, b"{}")
            .expect_err("unsupported attestation format must be rejected");
        assert_eq!(
            err.message(),
            "WebAuthn policy: attestation format 'enterprise' is not supported for statement signature verification"
        );
        assert_attestation_field_violation(
            &err,
            "fmt",
            "must be packed, tpm, android-key, or fido-u2f for attestation signature verification",
        );
    }

    #[test]
    fn fido_u2f_attestation_without_statement_signature_fails_closed() {
        let attestation = ParsedAttestationObject {
            fmt: "fido-u2f".to_string(),
            auth_data: fido_u2f_auth_data_with_ec2_key(),
            ver: None,
            alg: None,
            sig: None,
            x5c_der: Vec::new(),
            cert_info: None,
            pub_area: None,
        };
        let err = verify_attestation_statement_signature("fido-u2f", &attestation, b"{}")
            .expect_err("fido-u2f attestation without attStmt.sig must be rejected");
        assert_eq!(
            err.message(),
            "WebAuthn policy: fido-u2f attestation missing attStmt.sig"
        );
        assert_attestation_field_violation(
            &err,
            "attStmt.sig",
            "must be present for FIDO U2F attestation signature verification",
        );
    }

    #[test]
    fn android_key_attestation_without_statement_signature_fails_closed() {
        let attestation = ParsedAttestationObject {
            fmt: "android-key".to_string(),
            auth_data: vec![0u8; 37],
            ver: None,
            alg: Some(-7),
            sig: None,
            x5c_der: Vec::new(),
            cert_info: None,
            pub_area: None,
        };
        let err = verify_attestation_statement_signature("android-key", &attestation, b"{}")
            .expect_err("android-key attestation without attStmt.sig must be rejected");
        assert_eq!(
            err.message(),
            "WebAuthn policy: android-key attestation missing attStmt.sig"
        );
        assert_attestation_field_violation(
            &err,
            "attStmt.sig",
            "must be present for android-key attestation signature verification",
        );
    }

    #[test]
    fn android_key_attestation_malformed_leaf_certificate_carries_field_violation() {
        let attestation = ParsedAttestationObject {
            fmt: "android-key".to_string(),
            auth_data: vec![0u8; 37],
            ver: None,
            alg: Some(-7),
            sig: Some(vec![1, 2, 3]),
            x5c_der: vec![vec![0x30, 0x82, 0x01]],
            cert_info: None,
            pub_area: None,
        };
        let err = verify_attestation_statement_signature("android-key", &attestation, b"{}")
            .expect_err("malformed android-key attStmt.x5c must be rejected");
        assert!(
            err.message().starts_with(
                "WebAuthn policy: parse android-key attestation leaf certificate failed:"
            ),
            "unexpected error: {}",
            err.message()
        );
        assert_attestation_field_violation(
            &err,
            "attStmt.x5c",
            "must contain a valid DER-encoded android-key attestation leaf certificate",
        );
    }

    #[test]
    fn android_key_attestation_invalid_signature_carries_field_violation() {
        let attestation = ParsedAttestationObject {
            fmt: "android-key".to_string(),
            auth_data: vec![0u8; 37],
            ver: None,
            alg: Some(-257),
            sig: Some(vec![0u8; 256]),
            x5c_der: vec![test_self_signed_x509_der()],
            cert_info: None,
            pub_area: None,
        };
        let err = verify_attestation_statement_signature("android-key", &attestation, b"{}")
            .expect_err("invalid android-key attStmt.sig must be rejected");
        assert_eq!(
            err.message(),
            "WebAuthn policy: android-key attestation signature is invalid"
        );
        assert_attestation_field_violation(
            &err,
            "attStmt.sig",
            "must verify the android-key attestation statement signature",
        );
    }

    #[test]
    fn android_key_attestation_malformed_signature_carries_field_violation() {
        let attestation = ParsedAttestationObject {
            fmt: "android-key".to_string(),
            auth_data: vec![0u8; 37],
            ver: None,
            alg: Some(-257),
            sig: Some(Vec::new()),
            x5c_der: vec![test_self_signed_x509_der()],
            cert_info: None,
            pub_area: None,
        };
        let err = verify_attestation_statement_signature("android-key", &attestation, b"{}")
            .expect_err("malformed android-key attStmt.sig must be rejected");
        assert!(
            err.message()
                .starts_with("WebAuthn policy: verify android-key attestation signature failed:"),
            "unexpected error: {}",
            err.message()
        );
        assert_attestation_field_violation(
            &err,
            "attStmt.sig",
            "must be a well-formed android-key attestation statement signature",
        );
    }

    #[test]
    fn fido_u2f_attestation_requires_es256_credential_key() {
        let mut auth_data = fido_u2f_auth_data_with_ec2_key();
        let alg_value_offset = 37 + 16 + 2 + 3 + 1 + 2 + 1;
        auth_data[alg_value_offset] = 0x38; // -25, not ES256
        auth_data.insert(alg_value_offset + 1, 0x18);

        let attestation = ParsedAttestationObject {
            fmt: "fido-u2f".to_string(),
            auth_data,
            ver: None,
            alg: None,
            sig: Some(vec![1, 2, 3]),
            x5c_der: vec![vec![0x30, 0x82, 0x01]],
            cert_info: None,
            pub_area: None,
        };
        let err = fido_u2f_attestation_verification_data(&attestation, b"{}")
            .expect_err("non-ES256 U2F credential key must be rejected");
        assert_eq!(
            err.message(),
            "WebAuthn policy: fido-u2f attestation requires an EC2/P-256/ES256 credential key"
        );
        assert_attestation_field_violation(
            &err,
            "authData.credentialPublicKey",
            "must decode as an EC2/P-256/ES256 COSE credential public key",
        );
    }

    #[test]
    fn fido_u2f_attestation_malformed_leaf_certificate_carries_field_violation() {
        let attestation = ParsedAttestationObject {
            fmt: "fido-u2f".to_string(),
            auth_data: fido_u2f_auth_data_with_ec2_key(),
            ver: None,
            alg: None,
            sig: Some(vec![1, 2, 3]),
            x5c_der: vec![vec![0x30, 0x82, 0x01]],
            cert_info: None,
            pub_area: None,
        };
        let err = verify_attestation_statement_signature("fido-u2f", &attestation, b"{}")
            .expect_err("malformed FIDO U2F attStmt.x5c must be rejected");
        assert!(
            err.message().starts_with(
                "WebAuthn policy: parse fido-u2f attestation leaf certificate failed:"
            ),
            "unexpected error: {}",
            err.message()
        );
        assert_attestation_field_violation(
            &err,
            "attStmt.x5c",
            "must contain a valid DER-encoded FIDO U2F attestation leaf certificate",
        );
    }

    #[test]
    fn fido_u2f_attestation_invalid_signature_carries_field_violation() {
        let attestation = ParsedAttestationObject {
            fmt: "fido-u2f".to_string(),
            auth_data: fido_u2f_auth_data_with_ec2_key(),
            ver: None,
            alg: None,
            sig: Some(vec![0u8; 256]),
            x5c_der: vec![test_self_signed_x509_der()],
            cert_info: None,
            pub_area: None,
        };
        let err = verify_attestation_statement_signature("fido-u2f", &attestation, b"{}")
            .expect_err("invalid FIDO U2F attStmt.sig must be rejected");
        assert_eq!(
            err.message(),
            "WebAuthn policy: fido-u2f attestation signature is invalid"
        );
        assert_attestation_field_violation(
            &err,
            "attStmt.sig",
            "must verify the FIDO U2F attestation statement signature",
        );
    }

    #[test]
    fn fido_u2f_attestation_malformed_signature_carries_field_violation() {
        let attestation = ParsedAttestationObject {
            fmt: "fido-u2f".to_string(),
            auth_data: fido_u2f_auth_data_with_ec2_key(),
            ver: None,
            alg: None,
            sig: Some(Vec::new()),
            x5c_der: vec![test_self_signed_x509_der()],
            cert_info: None,
            pub_area: None,
        };
        let err = verify_attestation_statement_signature("fido-u2f", &attestation, b"{}")
            .expect_err("malformed FIDO U2F attStmt.sig must be rejected");
        assert!(
            err.message()
                .starts_with("WebAuthn policy: verify fido-u2f attestation signature failed:"),
            "unexpected error: {}",
            err.message()
        );
        assert_attestation_field_violation(
            &err,
            "attStmt.sig",
            "must be a well-formed FIDO U2F attestation statement signature",
        );
    }

    #[test]
    fn fido_u2f_attestation_missing_auth_data_field_carries_validation_detail() {
        let mut auth_data = vec![0u8; 37];
        auth_data[32] = 0x40;
        let attestation = ParsedAttestationObject {
            fmt: "fido-u2f".to_string(),
            auth_data,
            ver: None,
            alg: None,
            sig: Some(vec![1, 2, 3]),
            x5c_der: vec![vec![0x30, 0x82, 0x01]],
            cert_info: None,
            pub_area: None,
        };
        let err = fido_u2f_attestation_verification_data(&attestation, b"{}")
            .expect_err("missing U2F credential AAGUID must be rejected");
        assert_eq!(
            err.message(),
            "WebAuthn policy: fido-u2f attestation missing credential AAGUID"
        );
        assert_attestation_field_violation(
            &err,
            "authData.aaguid",
            "must include credential AAGUID for FIDO U2F attestation verification",
        );
    }

    #[test]
    fn fido_u2f_attestation_malformed_auth_data_carries_validation_detail() {
        let attestation = ParsedAttestationObject {
            fmt: "fido-u2f".to_string(),
            auth_data: vec![0u8; 36],
            ver: None,
            alg: None,
            sig: Some(vec![1, 2, 3]),
            x5c_der: vec![vec![0x30, 0x82, 0x01]],
            cert_info: None,
            pub_area: None,
        };
        let err = fido_u2f_attestation_verification_data(&attestation, b"{}")
            .expect_err("malformed U2F authenticator data must be rejected");
        assert_eq!(
            err.message(),
            "WebAuthn policy: fido-u2f attestation has malformed authenticator data"
        );
        assert_attestation_field_violation(
            &err,
            "authData",
            "must be at least 37 bytes for FIDO U2F attestation verification",
        );
    }

    #[test]
    fn tpm_attestation_invalid_version_carries_field_violation() {
        let attestation = ParsedAttestationObject {
            fmt: "tpm".to_string(),
            auth_data: vec![0u8; 37],
            ver: Some("1.2".to_string()),
            alg: Some(-257),
            sig: Some(vec![1, 2, 3]),
            x5c_der: vec![vec![0x30, 0x82, 0x01]],
            cert_info: Some(Vec::new()),
            pub_area: Some(vec![0x00, 0x01, 0x00, 0x0b]),
        };
        let err = verify_attestation_statement_signature("tpm", &attestation, b"{}")
            .expect_err("TPM attestation with unsupported attStmt.ver must be rejected");
        assert_eq!(
            err.message(),
            "WebAuthn policy: tpm attestation requires attStmt.ver = \"2.0\""
        );
        assert_attestation_field_violation(
            &err,
            "attStmt.ver",
            "must be \"2.0\" for TPM attestation signature verification",
        );
    }

    #[test]
    fn tpm_attestation_without_cert_info_fails_closed() {
        let attestation = ParsedAttestationObject {
            fmt: "tpm".to_string(),
            auth_data: vec![0u8; 37],
            ver: Some("2.0".to_string()),
            alg: Some(-257),
            sig: Some(vec![1, 2, 3]),
            x5c_der: vec![vec![0x30, 0x82, 0x01]],
            cert_info: None,
            pub_area: Some(vec![0x00, 0x01, 0x00, 0x0b]),
        };
        let err = verify_attestation_statement_signature("tpm", &attestation, b"{}")
            .expect_err("TPM attestation without attStmt.certInfo must be rejected");
        assert_eq!(
            err.message(),
            "WebAuthn policy: tpm attestation missing attStmt.certInfo"
        );
        assert_attestation_field_violation(
            &err,
            "attStmt.certInfo",
            "must be present for TPM attestation binding verification",
        );
    }

    #[test]
    fn tpm_attestation_cert_info_name_mismatch_carries_field_violation() {
        let client_data_json = br#"{"challenge":"tpm"}"#;
        let pub_area = tpm_test_pub_area();
        let wrong_pub_area_name = {
            let mut name = tpm_public_area_name(&pub_area).expect("pubArea name");
            name[2] ^= 0xff;
            name
        };
        let attestation_seed = ParsedAttestationObject {
            fmt: "tpm".to_string(),
            auth_data: vec![0xA9; 37],
            ver: Some("2.0".to_string()),
            alg: Some(-257),
            sig: Some(vec![1, 2, 3]),
            x5c_der: vec![vec![0x30, 0x82, 0x01]],
            cert_info: None,
            pub_area: None,
        };
        let extra_data = tpm_attestation_extra_data(&attestation_seed, client_data_json, -257)
            .expect("extraData");
        let attestation = ParsedAttestationObject {
            cert_info: Some(tpm_test_cert_info(&extra_data, &wrong_pub_area_name)),
            pub_area: Some(pub_area),
            ..attestation_seed
        };

        let err = verify_attestation_statement_signature("tpm", &attestation, client_data_json)
            .expect_err("TPM certInfo name mismatch must be rejected");
        assert_eq!(
            err.message(),
            "WebAuthn policy: tpm attestation certInfo name does not match pubArea"
        );
        assert_attestation_field_violation(
            &err,
            "attStmt.certInfo",
            "must name the same TPM public area as attStmt.pubArea",
        );
    }

    #[test]
    fn tpm_attestation_malformed_leaf_certificate_carries_field_violation() {
        let client_data_json = br#"{"challenge":"tpm"}"#;
        let mut attestation = ParsedAttestationObject {
            fmt: "tpm".to_string(),
            auth_data: vec![0xA9; 37],
            ver: Some("2.0".to_string()),
            alg: Some(-257),
            sig: Some(vec![1, 2, 3]),
            x5c_der: vec![vec![0x30, 0x82, 0x01]],
            cert_info: None,
            pub_area: None,
        };
        let pub_area = tpm_test_pub_area();
        let pub_area_name = tpm_public_area_name(&pub_area).expect("pubArea name");
        let extra_data =
            tpm_attestation_extra_data(&attestation, client_data_json, -257).expect("extraData");
        attestation.cert_info = Some(tpm_test_cert_info(&extra_data, &pub_area_name));
        attestation.pub_area = Some(pub_area);

        let err = verify_attestation_statement_signature("tpm", &attestation, client_data_json)
            .expect_err("malformed TPM attStmt.x5c must be rejected");
        assert!(
            err.message()
                .starts_with("WebAuthn policy: parse tpm attestation leaf certificate failed:"),
            "unexpected error: {}",
            err.message()
        );
        assert_attestation_field_violation(
            &err,
            "attStmt.x5c",
            "must contain a valid DER-encoded TPM attestation leaf certificate",
        );
    }

    #[test]
    fn tpm_attestation_invalid_signature_carries_field_violation() {
        let client_data_json = br#"{"challenge":"tpm"}"#;
        let mut attestation = ParsedAttestationObject {
            fmt: "tpm".to_string(),
            auth_data: vec![0xA9; 37],
            ver: Some("2.0".to_string()),
            alg: Some(-257),
            sig: Some(vec![0u8; 256]),
            x5c_der: vec![test_self_signed_x509_der()],
            cert_info: None,
            pub_area: None,
        };
        let pub_area = tpm_test_pub_area();
        let pub_area_name = tpm_public_area_name(&pub_area).expect("pubArea name");
        let extra_data =
            tpm_attestation_extra_data(&attestation, client_data_json, -257).expect("extraData");
        attestation.cert_info = Some(tpm_test_cert_info(&extra_data, &pub_area_name));
        attestation.pub_area = Some(pub_area);

        let err = verify_attestation_statement_signature("tpm", &attestation, client_data_json)
            .expect_err("invalid TPM attStmt.sig must be rejected");
        assert_eq!(
            err.message(),
            "WebAuthn policy: tpm attestation signature is invalid"
        );
        assert_attestation_field_violation(
            &err,
            "attStmt.sig",
            "must verify the TPM attestation statement signature",
        );
    }

    #[test]
    fn tpm_attestation_malformed_signature_carries_field_violation() {
        let client_data_json = br#"{"challenge":"tpm"}"#;
        let mut attestation = ParsedAttestationObject {
            fmt: "tpm".to_string(),
            auth_data: vec![0xA9; 37],
            ver: Some("2.0".to_string()),
            alg: Some(-257),
            sig: Some(Vec::new()),
            x5c_der: vec![test_self_signed_x509_der()],
            cert_info: None,
            pub_area: None,
        };
        let pub_area = tpm_test_pub_area();
        let pub_area_name = tpm_public_area_name(&pub_area).expect("pubArea name");
        let extra_data =
            tpm_attestation_extra_data(&attestation, client_data_json, -257).expect("extraData");
        attestation.cert_info = Some(tpm_test_cert_info(&extra_data, &pub_area_name));
        attestation.pub_area = Some(pub_area);

        let err = verify_attestation_statement_signature("tpm", &attestation, client_data_json)
            .expect_err("malformed TPM attStmt.sig must be rejected");
        assert!(
            err.message()
                .starts_with("WebAuthn policy: verify tpm attestation signature failed:"),
            "unexpected error: {}",
            err.message()
        );
        assert_attestation_field_violation(
            &err,
            "attStmt.sig",
            "must be a well-formed TPM attestation statement signature",
        );
    }

    #[test]
    fn deny_registration_when_attestation_conveyance_required_but_none() {
        let cred = soft_registration_credential(); // fmt == "none"
        let policy = WebAuthnPolicy::from_parts(
            Some("required".to_string()),
            Some("discouraged".to_string()),
            Some("direct".to_string()), // omits "none" ⇒ demands real attestation
        );
        let err = enforce_registration_policy(&policy, &cred)
            .expect_err("a none-attestation authenticator must be rejected");
        assert_webauthn_policy_detail(
            &err,
            "webauthn_registration_policy",
            "attestation_conveyance_not_allowed",
            "WebAuthn policy: attestation conveyance 'none' not permitted (tenant allows: direct)",
        );
    }

    #[test]
    fn allow_registration_when_conveyance_permits_none() {
        let cred = soft_registration_credential();
        let policy = WebAuthnPolicy::from_parts(
            Some("preferred".to_string()),
            Some("discouraged".to_string()),
            Some("none".to_string()),
        );
        enforce_registration_policy(&policy, &cred)
            .expect("a conformant none-attestation authenticator must pass");
    }

    #[test]
    fn deny_registration_when_resident_key_required_but_not_reported() {
        let cred = soft_registration_credential(); // no credProps ⇒ rk = None
        let policy = WebAuthnPolicy::from_parts(
            None,
            Some("required".to_string()),
            Some("none".to_string()),
        );
        let err = enforce_registration_policy(&policy, &cred)
            .expect_err("missing resident-key signal must be rejected");
        assert_webauthn_policy_detail(
            &err,
            "webauthn_registration_policy",
            "resident_key_required",
            "WebAuthn policy: tenant requires a resident (discoverable) key but the authenticator did not report credProps.rk = true",
        );
    }

    #[test]
    fn registration_uv_required_denies_uv_false_with_policy_detail() {
        let mut cred = soft_registration_credential();
        let mut attestation_object = Vec::new();
        attestation_object.push(0xa3); // map(3)
        attestation_object.extend_from_slice(&[0x63, b'f', b'm', b't']);
        attestation_object.extend_from_slice(&[0x64, b'n', b'o', b'n', b'e']);
        attestation_object.extend_from_slice(&[0x67, b'a', b't', b't', b'S', b't', b'm', b't']);
        attestation_object.push(0xa0); // empty attStmt map
        attestation_object
            .extend_from_slice(&[0x68, b'a', b'u', b't', b'h', b'D', b'a', b't', b'a']);
        attestation_object.extend_from_slice(&[0x58, 0x25]); // bytes(37)
        attestation_object.extend_from_slice(&[0u8; 37]); // flags byte 32 has UV=false
        cred.response.attestation_object = attestation_object.into();

        let policy = WebAuthnPolicy::from_parts(
            Some("required".to_string()),
            Some("discouraged".to_string()),
            Some("none".to_string()),
        );
        let err = enforce_registration_policy(&policy, &cred)
            .expect_err("UV-required policy must reject registration UV=false");
        assert_webauthn_policy_detail(
            &err,
            "webauthn_registration_policy",
            "registration_user_verification_required",
            "WebAuthn policy: tenant requires user verification but the registration authenticator-data UV flag was not set",
        );
    }

    #[test]
    fn assertion_uv_required_denies_uv_false_and_allows_uv_true() {
        let policy = WebAuthnPolicy::from_parts(Some("required".to_string()), None, None);
        let err = enforce_assertion_policy(&policy, false)
            .expect_err("UV-required policy must reject a UV=false assertion");
        assert_webauthn_policy_detail(
            &err,
            "webauthn_assertion_policy",
            "assertion_user_verification_required",
            "WebAuthn policy: tenant requires user verification but the assertion reported UV = false",
        );
        enforce_assertion_policy(&policy, true).expect("UV present satisfies the policy");
    }

    #[test]
    fn preferred_and_discouraged_never_deny() {
        let cred = soft_registration_credential();
        // All-permissive (env-default-equivalent) policy: nothing rejects.
        let policy = WebAuthnPolicy::from_parts(None, None, None);
        enforce_registration_policy(&policy, &cred).expect("default policy is permissive");
        enforce_assertion_policy(&policy, false).expect("UV preferred never denies");
    }
}
