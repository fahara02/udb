//! 0.6.6 invitations: an administrator invites a person into a tenant with a
//! set of roles; the invitee proves the invitation's channel and gets an
//! ACTIVE account, signed in.
//!
//! - CreateInvitation / ListInvitations / RevokeInvitation / ResendInvitation
//!   (bearer, `udb:authn:manage-invitations`, the caller's own tenant only; a
//!   cross-tenant admin may name any tenant).
//! - GetInvitation / StartInvitationAcceptance / AcceptInvitation (public).
//!   An unknown, expired, revoked or accepted invitation, or a wrong token, is
//!   ONE refusal ([`invitation_unavailable_status`]), so the public surface
//!   reveals nothing about which invitations exist.
//!
//! The accept token travels only in the link; the row stores its keyed hash
//! and ResendInvitation replaces it. One-time codes (SMS/EMAIL) and passkey
//! challenges belong to a user row, so the first StartInvitationAcceptance
//! creates the invitee's account as PENDING_VERIFICATION with a random
//! password and no contact (nobody can sign in to it) and records it on the
//! invitation; AcceptInvitation activates that same account. Revoking the
//! invitation, or replacing a lapsed one, deletes it.
//!
//! Acceptance commits the account, its verified channel, the role bindings
//! (plus an authz revision so every node reloads its snapshot), the ACCEPTED
//! invitation and the audit events in ONE transaction, then signs in through
//! `complete_sign_in` (`auth_method` "invitation").

use sqlx::Row;

use super::*;
use crate::runtime::native_catalog::native_model;

use super::super::grants::{
    caller_updated_by, event_auth_method, map_err, next_page_token, page_window,
};

/// Manifest key `native_model` resolves the invitation table from.
const INVITATION_MSG: &str = "udb.core.authn.entity.v1.Invitation";
const USER_MSG: &str = "udb.core.authn.entity.v1.User";
const EXTERNAL_IDENTITY_MSG: &str = "udb.core.idp.entity.v1.ExternalIdentity";
const TENANT_MSG: &str = "udb.core.tenant.entity.v1.Tenant";

/// The scope every invitation-management RPC requires.
pub(super) const MANAGE_INVITATIONS_SCOPE: &str = "udb:authn:manage-invitations";

pub(super) const TOPIC_INVITATION_CREATED: &str = "udb.authn.invitation.created.v1";
pub(super) const TOPIC_INVITATION_RESENT: &str = "udb.authn.invitation.resent.v1";
pub(super) const TOPIC_INVITATION_ACCEPTED: &str = "udb.authn.invitation.accepted.v1";
pub(super) const TOPIC_INVITATION_REVOKED: &str = "udb.authn.invitation.revoked.v1";

/// NotificationService event (template) of the invitation message.
const INVITATION_EVENT_TYPE: &str = "authn.invitation";

const STATUS_PENDING: &str = "PENDING";
const STATUS_ACCEPTED: &str = "ACCEPTED";
const STATUS_REVOKED: &str = "REVOKED";
const STATUS_EXPIRED: &str = "EXPIRED";

const DEFAULT_TTL_SECS: u64 = 7 * 24 * 60 * 60;
const MIN_TTL_SECS: u64 = 5 * 60;
const MAX_TTL_SECS: u64 = 30 * 24 * 60 * 60;

/// At most this many invitation messages per invitation (Create counts as one).
const MAX_SENDS: i32 = 5;
/// Minimum gap between two invitation messages.
const RESEND_MIN_INTERVAL_SECS: u64 = 60;

const MAX_ROLES: usize = 32;
const ROLE_CODE_MAX_CHARS: usize = 120;

/// Accept tokens look like `inv_<64 hex>` (two v4 UUIDs: 244 random bits).
const ACCEPT_TOKEN_PREFIX: &str = "inv_";

/// Label of a passkey registered while accepting an invitation.
const INVITATION_PASSKEY_LABEL: &str = "invitation";

// ── Statuses ─────────────────────────────────────────────────────────────────

/// The ONE refusal for every unusable invitation on the public surface:
/// unknown id, wrong token, expired, revoked or already accepted. Identical so
/// a caller learns nothing about which invitations exist.
fn invitation_unavailable_status() -> Status {
    crate::runtime::executor_utils::policy_status_with_code(
        tonic::Code::PermissionDenied,
        "invitation_acceptance",
        "invitation_unavailable",
        "this invitation cannot be used: it may be unknown, expired, revoked or already accepted",
    )
}

fn invitation_not_found_status() -> Status {
    authn_schema_not_found_status(
        "invitation_lookup",
        "authn_invitation_not_found",
        "invitation not found",
    )
}

fn invitation_invalid_fields<I, F, D>(message: impl Into<String>, fields: I) -> Status
where
    I: IntoIterator<Item = (F, D)>,
    F: Into<String>,
    D: Into<String>,
{
    crate::runtime::executor_utils::invalid_argument_fields(message, fields)
}

fn invitation_precondition_fields<I, F, D>(message: impl Into<String>, fields: I) -> Status
where
    I: IntoIterator<Item = (F, D)>,
    F: Into<String>,
    D: Into<String>,
{
    crate::runtime::executor_utils::failed_precondition_fields(message, fields)
}

fn reserved_role_status(role: &str) -> Status {
    crate::runtime::executor_utils::policy_status_with_code(
        tonic::Code::PermissionDenied,
        "create_invitation",
        "reserved_platform_role_authority_required",
        format!("role '{role}' is a reserved platform role and cannot be granted by an invitation"),
    )
}

fn owner_authority_required_status() -> Status {
    crate::runtime::executor_utils::policy_status_with_code(
        tonic::Code::PermissionDenied,
        "create_invitation",
        "organization_owner_authority_required",
        "only an organization_owner or a cross-tenant admin may invite an organization_owner",
    )
}

/// A tenant that requires multi-factor sign-in cannot admit an account on a
/// single code: its invitations must prove a passkey or an identity provider.
fn tenant_mfa_requires_strong_channel_status() -> Status {
    crate::runtime::executor_utils::policy_status_with_code(
        tonic::Code::FailedPrecondition,
        "invitation_channel",
        "tenant_mfa_requires_passkey_or_oidc_invitation",
        "this tenant requires multi-factor sign-in: invite with channel PASSKEY or OIDC",
    )
}

/// Governed authz mode: role bindings need the governed approval flow, so an
/// invitation cannot carry roles while it is on.
fn invitation_roles_governed_status() -> Status {
    crate::runtime::executor_utils::policy_status_with_code(
        tonic::Code::FailedPrecondition,
        "invitation_roles",
        "invitation_roles_require_governed_approval",
        "governed authz mode is on: an invitation cannot bind roles; invite without roles and grant them through the governed AssignRole/approval flow after acceptance",
    )
}

/// Roles may ride on an invitation only outside governed authz mode.
fn require_roles_outside_governed_mode(roles: &[String], governed: bool) -> Result<(), Status> {
    if governed && !roles.is_empty() {
        return Err(invitation_roles_governed_status());
    }
    Ok(())
}

fn invitation_token_secret_status() -> Status {
    authn_capability_status(
        "invitations",
        "session_hash_secret",
        "invitations need UDB_SESSION_HASH_SECRET to protect their accept tokens",
    )
}

fn invitation_password_secret_status() -> Status {
    authn_capability_status(
        "native_user_passwords",
        "password_hash_secret",
        "native user passwords require UDB_PASSWORD_HASH_SECRET or UDB_SESSION_HASH_SECRET",
    )
}

fn invalid_invitation_code_status() -> Status {
    crate::runtime::executor_utils::unauthenticated_status(
        "invalid_invitation_code",
        "the code is wrong, expired or already used; ask for a new one",
    )
}

fn invalid_invitation_identity_status() -> Status {
    crate::runtime::executor_utils::unauthenticated_status(
        "invalid_invitation_identity",
        "the identity provider sign-in does not prove this invitation",
    )
}

fn username_taken_status() -> Status {
    crate::runtime::executor_utils::schema_status(
        tonic::Code::AlreadyExists,
        "authn",
        "accept_invitation",
        "authn_username_taken",
        "username is already taken; choose another",
    )
}

fn resend_throttled_status(ready_at: u64, now: u64) -> Status {
    let retry_after_secs = ready_at.saturating_sub(now).max(1);
    let retry_after_ms = i64::try_from(retry_after_secs.saturating_mul(1_000)).unwrap_or(i64::MAX);
    crate::runtime::error_reasons::annotate(
        crate::runtime::executor_utils::quota_status(
            "authn",
            "invitation_resend",
            retry_after_ms,
            format!("the invitation was just sent; retry in {retry_after_secs} seconds"),
        ),
        crate::runtime::error_reasons::RATE_LIMITED,
        None,
        None,
    )
}

fn send_limit_reached_status() -> Status {
    crate::runtime::executor_utils::policy_status_with_code(
        tonic::Code::ResourceExhausted,
        "invitation_resend",
        "invitation_send_limit_reached",
        format!(
            "this invitation was already sent {MAX_SENDS} times; revoke it and create a new one"
        ),
    )
}

// ── Pure validation ──────────────────────────────────────────────────────────

/// What the invitee proves on acceptance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InvitationChannel {
    Sms,
    Email,
    Oidc,
    Passkey,
}

impl InvitationChannel {
    fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_uppercase().as_str() {
            "SMS" => Some(Self::Sms),
            "EMAIL" => Some(Self::Email),
            "OIDC" => Some(Self::Oidc),
            "PASSKEY" => Some(Self::Passkey),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Sms => "SMS",
            Self::Email => "EMAIL",
            Self::Oidc => "OIDC",
            Self::Passkey => "PASSKEY",
        }
    }

    /// The OTP / notification delivery channel, for the channels that send a
    /// message (SMS, EMAIL). OIDC and PASSKEY invitations send nothing.
    fn delivery_channel(self) -> Option<&'static str> {
        match self {
            Self::Sms => Some("sms"),
            Self::Email => Some("email"),
            Self::Oidc | Self::Passkey => None,
        }
    }
}

fn parse_channel(raw: &str) -> Result<InvitationChannel, Status> {
    InvitationChannel::parse(raw).ok_or_else(|| {
        invitation_invalid_fields(
            "channel must be SMS, EMAIL, OIDC or PASSKEY",
            [("channel", "must be one of SMS, EMAIL, OIDC, PASSKEY")],
        )
    })
}

fn is_plausible_email(email: &str) -> bool {
    let Some((local, domain)) = email.split_once('@') else {
        return false;
    };
    email.len() <= 254
        && !local.is_empty()
        && !domain.contains('@')
        && domain.contains('.')
        && !domain.starts_with('.')
        && !domain.ends_with('.')
        && !email.chars().any(|c| c.is_whitespace() || c.is_control())
}

/// The stored address for `channel`: an E.164 phone (SMS), a lowercase email
/// (EMAIL), or empty (OIDC, PASSKEY: nothing is sent to them).
fn normalized_address(channel: InvitationChannel, raw: &str) -> Result<String, Status> {
    let raw = raw.trim();
    match channel {
        InvitationChannel::Sms => {
            if super::core::is_e164_phone(raw) {
                Ok(raw.to_string())
            } else {
                Err(invitation_invalid_fields(
                    "address must be an E.164 phone number for an SMS invitation",
                    [(
                        "address",
                        "must be an E.164 number: + then 7 to 15 digits, not starting with 0",
                    )],
                ))
            }
        }
        InvitationChannel::Email => {
            let email = raw.to_ascii_lowercase();
            if is_plausible_email(&email) {
                Ok(email)
            } else {
                Err(invitation_invalid_fields(
                    "address must be an email address for an EMAIL invitation",
                    [("address", "must be an email address")],
                ))
            }
        }
        InvitationChannel::Oidc | InvitationChannel::Passkey => {
            if raw.is_empty() {
                Ok(String::new())
            } else {
                Err(invitation_invalid_fields(
                    "address must be empty for OIDC and PASSKEY invitations",
                    [("address", "must be empty for OIDC and PASSKEY invitations")],
                ))
            }
        }
    }
}

/// `idp_provider_id`: a provider UUID for OIDC, empty for every other channel.
fn validated_provider(channel: InvitationChannel, raw: &str) -> Result<String, Status> {
    let raw = raw.trim();
    if channel != InvitationChannel::Oidc {
        if raw.is_empty() {
            return Ok(String::new());
        }
        return Err(invitation_invalid_fields(
            "idp_provider_id is only used by OIDC invitations",
            [("idp_provider_id", "must be empty unless channel is OIDC")],
        ));
    }
    Uuid::parse_str(raw)
        .map(|provider| provider.to_string())
        .map_err(|_| {
            invitation_invalid_fields(
                "idp_provider_id is required for an OIDC invitation",
                [(
                    "idp_provider_id",
                    "must be the id of one of the tenant's identity providers",
                )],
            )
        })
}

/// Role codes: trimmed, non-empty, at most 120 characters with no whitespace,
/// de-duplicated case-insensitively (first spelling kept), at most 32. A
/// reserved platform role is refused outright: no invitation can mint
/// platform authority.
fn validated_roles(raw: &[String]) -> Result<Vec<String>, Status> {
    let mut roles: Vec<String> = Vec::new();
    for role in raw {
        let role = role.trim();
        if role.is_empty()
            || role.chars().count() > ROLE_CODE_MAX_CHARS
            || role.chars().any(|c| c.is_whitespace() || c.is_control())
        {
            return Err(invitation_invalid_fields(
                "roles must be role codes",
                [(
                    "roles",
                    "every role must be a non-empty role code of at most 120 characters with no whitespace",
                )],
            ));
        }
        if crate::runtime::service::method_security::is_platform_authority_role(role) {
            return Err(reserved_role_status(role));
        }
        if !roles.iter().any(|seen| seen.eq_ignore_ascii_case(role)) {
            roles.push(role.to_string());
        }
    }
    if roles.len() > MAX_ROLES {
        return Err(invitation_invalid_fields(
            format!("roles must contain at most {MAX_ROLES} role codes"),
            [("roles", "must contain at most 32 distinct role codes")],
        ));
    }
    Ok(roles)
}

/// `organization_owner` projects the tenant superuser scopes, so only a caller
/// who already holds it, or a cross-tenant admin, may invite one (the same
/// rule AssignRole applies). In-process calls with no claim context are
/// trusted, as for every admin RPC.
fn require_owner_authority(roles: &[String]) -> Result<(), Status> {
    let owner = super::super::ORG_OWNER_ROLE_CODE;
    if !roles.iter().any(|role| role.eq_ignore_ascii_case(owner))
        || !crate::runtime::service::method_security::claim_context_present()
    {
        return Ok(());
    }
    let claim = crate::runtime::service::method_security::current_claim_context();
    let holds_owner = claim
        .roles
        .iter()
        .any(|held| held.trim().eq_ignore_ascii_case(owner));
    if claim.is_cross_tenant_admin() || holds_owner {
        Ok(())
    } else {
        tracing::warn!(
            target: "udb.audit.authz",
            subject = %claim.subject,
            tenant = %claim.tenant_id,
            "DENY: inviting an organization_owner without holding it"
        );
        Err(owner_authority_required_status())
    }
}

/// Link lifetime: 0 = 7 days, else between 5 minutes and 30 days.
fn invitation_ttl_secs(raw: i64) -> Result<u64, Status> {
    if raw == 0 {
        return Ok(DEFAULT_TTL_SECS);
    }
    if raw < MIN_TTL_SECS as i64 || raw > MAX_TTL_SECS as i64 {
        return Err(invitation_invalid_fields(
            "expires_in_seconds must be between 300 and 2592000 (30 days)",
            [(
                "expires_in_seconds",
                "must be 0 (7 days) or between 300 and 2592000",
            )],
        ));
    }
    Ok(raw as u64)
}

fn bounded_text(raw: &str, field: &'static str, max_bytes: usize) -> Result<String, Status> {
    let value = raw.trim();
    if value.len() > max_bytes || value.chars().any(char::is_control) {
        return Err(invitation_invalid_fields(
            format!("{field} is malformed"),
            [(
                field,
                format!("must be at most {max_bytes} bytes and contain no control characters"),
            )],
        ));
    }
    Ok(value.to_string())
}

/// The new account's username, as the users table accepts it:
/// `^[a-z][a-z0-9._-]{2,79}$` after lowercasing.
fn validated_username(raw: &str) -> Result<String, Status> {
    let username = raw.trim().to_ascii_lowercase();
    let mut chars = username.chars();
    let valid = (3..=80).contains(&username.len())
        && chars.next().is_some_and(|c| c.is_ascii_lowercase())
        && chars
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, '.' | '_' | '-'));
    if valid {
        Ok(username)
    } else {
        Err(invitation_invalid_fields(
            "username must start with a letter and use 3 to 80 of a-z 0-9 . _ -",
            [(
                "username",
                "must be 3 to 80 characters: a lowercase letter, then a-z 0-9 . _ -",
            )],
        ))
    }
}

/// What the invitee is shown of the address: `+88*******678`,
/// `a***@example.com`; empty for OIDC and PASSKEY.
fn masked_address(channel: InvitationChannel, address: &str) -> String {
    match channel {
        InvitationChannel::Sms => {
            let chars: Vec<char> = address.chars().collect();
            if chars.len() <= 6 {
                return "*".repeat(chars.len());
            }
            let head: String = chars[..3].iter().collect();
            let tail: String = chars[chars.len() - 3..].iter().collect();
            format!("{head}{}{tail}", "*".repeat(chars.len() - 6))
        }
        InvitationChannel::Email => match address.split_once('@') {
            Some((local, domain)) => {
                let first: String = local.chars().take(1).collect();
                format!("{first}***@{domain}")
            }
            None => String::new(),
        },
        InvitationChannel::Oidc | InvitationChannel::Passkey => String::new(),
    }
}

fn new_accept_token() -> String {
    format!(
        "{ACCEPT_TOKEN_PREFIX}{}{}",
        Uuid::new_v4().simple(),
        Uuid::new_v4().simple()
    )
}

/// `<base>/invite?invitation_id=..&token=..`, or empty when the tenant has no
/// `magic_link_base_url`. Both values are URL-safe (UUID, `inv_<hex>`).
fn invitation_link(base: &str, invitation_id: &str, token: &str) -> String {
    let base = base.trim().trim_end_matches('/');
    if base.is_empty() {
        return String::new();
    }
    format!("{base}/invite?invitation_id={invitation_id}&token={token}")
}

/// A status filter for ListInvitations: empty (all) or one of the four.
fn validated_status_filter(raw: &str) -> Result<String, Status> {
    let status = raw.trim().to_ascii_uppercase();
    match status.as_str() {
        "" | STATUS_PENDING | STATUS_ACCEPTED | STATUS_REVOKED | STATUS_EXPIRED => Ok(status),
        _ => Err(invitation_invalid_fields(
            "status must be PENDING, ACCEPTED, REVOKED or EXPIRED",
            [(
                "status",
                "must be empty or one of PENDING, ACCEPTED, REVOKED, EXPIRED",
            )],
        )),
    }
}

/// LoginResponse → AcceptInvitationResponse (same field names and numbers).
fn accept_invitation_response(
    signed_in: authn_pb::LoginResponse,
) -> authn_pb::AcceptInvitationResponse {
    authn_pb::AcceptInvitationResponse {
        user_id: signed_in.user_id,
        session_id: signed_in.session_id,
        access_token: signed_in.access_token,
        refresh_token: signed_in.refresh_token,
        access_token_expires_in: signed_in.access_token_expires_in,
        session_token: signed_in.session_token,
        csrf_token: signed_in.csrf_token,
        mfa_required: signed_in.mfa_required,
        mfa_otp_id: signed_in.mfa_otp_id,
        refresh_token_expires_in: signed_in.refresh_token_expires_in,
        session_public_id: signed_in.session_public_id,
    }
}

// ── Store ────────────────────────────────────────────────────────────────────

/// Runtime view of one `udb_authn.invitations` row.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct InvitationRecord {
    invitation_id: String,
    tenant_id: String,
    project_id: String,
    channel: String,
    address: String,
    idp_provider_id: String,
    roles: Vec<String>,
    full_name: String,
    status: String,
    token_hash: String,
    expires_at_unix: u64,
    created_by: String,
    invitee_user_id: String,
    accepted_user_id: String,
    accepted_at_unix: u64,
    send_count: i32,
    last_sent_at_unix: u64,
    revision: i64,
    created_at_unix: u64,
    updated_at_unix: u64,
    locale: String,
}

impl InvitationRecord {
    fn channel(&self) -> Option<InvitationChannel> {
        InvitationChannel::parse(&self.channel)
    }

    /// PENDING and not yet expired at `now`: the only state that accepts.
    fn is_open(&self, now: u64) -> bool {
        self.status == STATUS_PENDING && self.expires_at_unix > now
    }

    /// The status callers see: a PENDING row past its expiry reads EXPIRED.
    fn reported_status(&self, now: u64) -> &str {
        if self.status == STATUS_PENDING && self.expires_at_unix <= now {
            STATUS_EXPIRED
        } else {
            &self.status
        }
    }

    fn masked_address(&self) -> String {
        self.channel()
            .map(|channel| masked_address(channel, &self.address))
            .unwrap_or_default()
    }

    fn to_info(&self, now: u64) -> authn_pb::InvitationInfo {
        authn_pb::InvitationInfo {
            invitation_id: self.invitation_id.clone(),
            tenant_id: self.tenant_id.clone(),
            project_id: self.project_id.clone(),
            channel: self.channel.clone(),
            address: self.address.clone(),
            idp_provider_id: self.idp_provider_id.clone(),
            roles: self.roles.clone(),
            full_name: self.full_name.clone(),
            status: self.reported_status(now).to_string(),
            expires_at: timestamp_from_unix(self.expires_at_unix),
            created_by: self.created_by.clone(),
            accepted_user_id: self.accepted_user_id.clone(),
            accepted_at: timestamp_from_unix(self.accepted_at_unix),
            send_count: self.send_count,
            last_sent_at: timestamp_from_unix(self.last_sent_at_unix),
            revision: self.revision,
            created_at: timestamp_from_unix(self.created_at_unix),
            updated_at: timestamp_from_unix(self.updated_at_unix),
            locale: self.locale.clone(),
        }
    }
}

fn invitation_columns() -> Vec<&'static str> {
    vec![
        "invitation_id",
        "tenant_id",
        "project_id",
        "channel",
        "address",
        "idp_provider_id",
        "roles_json",
        "full_name",
        "status",
        "token_hash",
        "expires_at",
        "created_by",
        "invitee_user_id",
        "accepted_user_id",
        "accepted_at",
        "revoked_by",
        "revoke_reason",
        "send_count",
        "last_sent_at",
        "revision",
        "created_at",
        "updated_at",
        "locale",
    ]
}

fn invitation_select_clause() -> String {
    let m = native_model(INVITATION_MSG, &invitation_columns());
    [
        m.text_or_empty_as("invitation_id", "invitation_id"),
        m.text_or_empty_as("tenant_id", "tenant_id"),
        m.text_or_empty_as("project_id", "project_id"),
        m.text_or_empty_as("channel", "channel"),
        m.text_or_empty_as("address", "address"),
        m.text_or_empty_as("idp_provider_id", "idp_provider_id"),
        m.json_text_as("roles_json", "roles_json"),
        m.text_or_empty_as("full_name", "full_name"),
        m.text_or_empty_as("status", "status"),
        m.text_or_empty_as("token_hash", "token_hash"),
        m.timestamp_unix_as("expires_at", "expires_at_unix"),
        m.text_or_empty_as("created_by", "created_by"),
        m.text_or_empty_as("invitee_user_id", "invitee_user_id"),
        m.text_or_empty_as("accepted_user_id", "accepted_user_id"),
        m.timestamp_unix_as("accepted_at", "accepted_at_unix"),
        format!("{} AS send_count", m.q("send_count")),
        m.timestamp_unix_as("last_sent_at", "last_sent_at_unix"),
        format!("{} AS revision", m.q("revision")),
        m.timestamp_unix_as("created_at", "created_at_unix"),
        m.timestamp_unix_as("updated_at", "updated_at_unix"),
        m.text_or_empty_as("locale", "locale"),
    ]
    .join(", ")
}

/// A row that fails to decode degrades to empty fields — an empty status is
/// never PENDING and an empty token hash never matches, so every consumer
/// fails closed on it.
fn invitation_row_from(row: &sqlx::postgres::PgRow) -> InvitationRecord {
    let unix = |column: &str| row.try_get::<i64, _>(column).unwrap_or(0).max(0) as u64;
    InvitationRecord {
        invitation_id: row.try_get("invitation_id").unwrap_or_default(),
        tenant_id: row.try_get("tenant_id").unwrap_or_default(),
        project_id: row.try_get("project_id").unwrap_or_default(),
        channel: row.try_get("channel").unwrap_or_default(),
        address: row.try_get("address").unwrap_or_default(),
        idp_provider_id: row.try_get("idp_provider_id").unwrap_or_default(),
        roles: serde_json::from_str(
            &row.try_get::<String, _>("roles_json")
                .unwrap_or_else(|_| "[]".to_string()),
        )
        .unwrap_or_default(),
        full_name: row.try_get("full_name").unwrap_or_default(),
        status: row.try_get("status").unwrap_or_default(),
        token_hash: row.try_get("token_hash").unwrap_or_default(),
        expires_at_unix: unix("expires_at_unix"),
        created_by: row.try_get("created_by").unwrap_or_default(),
        invitee_user_id: row.try_get("invitee_user_id").unwrap_or_default(),
        accepted_user_id: row.try_get("accepted_user_id").unwrap_or_default(),
        accepted_at_unix: unix("accepted_at_unix"),
        send_count: row.try_get("send_count").unwrap_or(0),
        last_sent_at_unix: unix("last_sent_at_unix"),
        revision: row.try_get("revision").unwrap_or(0),
        created_at_unix: unix("created_at_unix"),
        updated_at_unix: unix("updated_at_unix"),
        locale: row.try_get("locale").unwrap_or_default(),
    }
}

async fn get_invitation_by_id<'c, E>(
    executor: E,
    invitation_id: &Uuid,
) -> Result<Option<InvitationRecord>, Status>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let m = native_model(INVITATION_MSG, &["invitation_id"]);
    let sql = format!(
        "SELECT {cols} FROM {rel} WHERE {iid} = $1 LIMIT 1",
        cols = invitation_select_clause(),
        rel = m.relation,
        iid = m.q("invitation_id"),
    );
    let row = sqlx::query(&sql)
        .bind(invitation_id)
        .fetch_optional(executor)
        .await
        .map_err(map_err("invitation get failed"))?;
    Ok(row.map(|row| invitation_row_from(&row)))
}

/// Mark PENDING-but-lapsed invitations for the same (tenant, channel, address)
/// EXPIRED, so a new invitation can replace them. Returns the invitee
/// accounts they had started, which the caller deletes.
async fn expire_lapsed_invitations(
    conn: &mut sqlx::PgConnection,
    tenant_id: &str,
    channel: &str,
    address: &str,
) -> Result<Vec<String>, Status> {
    let m = native_model(INVITATION_MSG, &invitation_columns());
    let sql = format!(
        "UPDATE {rel} SET {status} = $4, {revision} = {revision} + 1, {updated} = now() \
         WHERE {tenant} = $1 AND {channel} = $2 AND {address} = $3 \
           AND {status} = $5 AND {expires} <= now() \
         RETURNING COALESCE({invitee}::TEXT, '') AS invitee_user_id",
        rel = m.relation,
        status = m.q("status"),
        revision = m.q("revision"),
        updated = m.q("updated_at"),
        tenant = m.q("tenant_id"),
        channel = m.q("channel"),
        address = m.q("address"),
        expires = m.q("expires_at"),
        invitee = m.q("invitee_user_id"),
    );
    let rows = sqlx::query(&sql)
        .bind(tenant_id)
        .bind(channel)
        .bind(address)
        .bind(STATUS_EXPIRED)
        .bind(STATUS_PENDING)
        .fetch_all(&mut *conn)
        .await
        .map_err(map_err("invitation expiry failed"))?;
    Ok(rows
        .iter()
        .filter_map(|row| row.try_get::<String, _>("invitee_user_id").ok())
        .filter(|user_id| !user_id.is_empty())
        .collect())
}

/// The columns of a new invitation.
struct NewInvitation<'a> {
    tenant_id: &'a str,
    project_id: &'a str,
    channel: InvitationChannel,
    address: &'a str,
    idp_provider_id: &'a str,
    roles: &'a [String],
    full_name: &'a str,
    token_hash: &'a str,
    expires_at_unix: u64,
    created_by: &'a str,
    locale: &'a str,
}

async fn insert_invitation(
    conn: &mut sqlx::PgConnection,
    new: &NewInvitation<'_>,
) -> Result<InvitationRecord, Status> {
    let m = native_model(INVITATION_MSG, &invitation_columns());
    // A channel that sends a message counts the one CreateInvitation sends.
    let send_count: i32 = i32::from(new.channel.delivery_channel().is_some());
    // Raw escape hatch: the partial unique index on the open address must map
    // to a typed FailedPrecondition on `address`.
    let sql = format!(
        "INSERT INTO {rel} \
            ({iid}, {tenant}, {project}, {channel}, {address}, {provider}, {roles}, {full_name}, \
             {status}, {token_hash}, {expires}, {created_by}, {send_count}, {last_sent}, {revision}, {locale}) \
         VALUES (gen_random_uuid(), $1, $2, $3, $4, $5, $6::JSONB, $7, $8, $9, \
                 to_timestamp($10::DOUBLE PRECISION), $11, $12::INTEGER, \
                 CASE WHEN $12::INTEGER > 0 THEN now() ELSE NULL END, 1, $13) \
         RETURNING {cols}",
        rel = m.relation,
        iid = m.q("invitation_id"),
        tenant = m.q("tenant_id"),
        project = m.q("project_id"),
        channel = m.q("channel"),
        address = m.q("address"),
        provider = m.q("idp_provider_id"),
        roles = m.q("roles_json"),
        full_name = m.q("full_name"),
        status = m.q("status"),
        token_hash = m.q("token_hash"),
        expires = m.q("expires_at"),
        created_by = m.q("created_by"),
        send_count = m.q("send_count"),
        last_sent = m.q("last_sent_at"),
        revision = m.q("revision"),
        locale = m.q("locale"),
        cols = invitation_select_clause(),
    );
    let row = sqlx::query(&sql)
        .bind(new.tenant_id)
        .bind(new.project_id)
        .bind(new.channel.as_str())
        .bind(new.address)
        .bind(new.idp_provider_id)
        .bind(serde_json::to_string(new.roles).unwrap_or_else(|_| "[]".to_string()))
        .bind(new.full_name)
        .bind(STATUS_PENDING)
        .bind(new.token_hash)
        .bind(new.expires_at_unix as f64)
        .bind(new.created_by)
        .bind(send_count)
        .bind(new.locale)
        .fetch_one(&mut *conn)
        .await
        .map_err(
            |err| match super::super::grants::unique_violation_constraint(&err) {
                Some(_) => invitation_precondition_fields(
                    "an open invitation already exists for this address; resend or revoke it",
                    [(
                        "address",
                        "must not already have a PENDING invitation in this tenant",
                    )],
                ),
                None => map_err("invitation insert failed")(err),
            },
        )?;
    Ok(invitation_row_from(&row))
}

async fn list_invitations(
    pool: &PgPool,
    tenant_id: &str,
    status: &str,
    limit: i64,
    offset: i64,
) -> Result<Vec<InvitationRecord>, Status> {
    let m = native_model(INVITATION_MSG, &invitation_columns());
    let sql = format!(
        "SELECT {cols} FROM {rel} \
         WHERE {tenant} = $1 \
           AND ($2 = '' OR (CASE WHEN {status} = '{pending}' AND {expires} <= now() \
                                 THEN '{expired}' ELSE {status} END) = $2) \
         ORDER BY {created} DESC, {iid} ASC LIMIT $3 OFFSET $4",
        cols = invitation_select_clause(),
        rel = m.relation,
        tenant = m.q("tenant_id"),
        status = m.q("status"),
        expires = m.q("expires_at"),
        pending = STATUS_PENDING,
        expired = STATUS_EXPIRED,
        created = m.q("created_at"),
        iid = m.q("invitation_id"),
    );
    let rows = sqlx::query(&sql)
        .bind(tenant_id)
        .bind(status)
        .bind(limit.max(1))
        .bind(offset.max(0))
        .fetch_all(pool)
        .await
        .map_err(map_err("invitation list failed"))?;
    Ok(rows.iter().map(invitation_row_from).collect())
}

/// Rotate the accept token of an open invitation and count the message, only
/// while it is under the send limit and outside the resend interval. `None`
/// when nothing matched (closed, throttled, or a concurrent resend won).
async fn record_resend(
    pool: &PgPool,
    invitation_id: &Uuid,
    token_hash: &str,
) -> Result<Option<InvitationRecord>, Status> {
    let m = native_model(INVITATION_MSG, &invitation_columns());
    let sql = format!(
        "UPDATE {rel} SET {token_hash} = $2, {send_count} = {send_count} + 1, \
            {last_sent} = now(), {revision} = {revision} + 1, {updated} = now() \
         WHERE {iid} = $1 AND {status} = $3 AND {expires} > now() AND {send_count} < $4 \
           AND ({last_sent} IS NULL OR {last_sent} <= now() - make_interval(secs => $5::DOUBLE PRECISION)) \
         RETURNING {cols}",
        rel = m.relation,
        token_hash = m.q("token_hash"),
        send_count = m.q("send_count"),
        last_sent = m.q("last_sent_at"),
        revision = m.q("revision"),
        updated = m.q("updated_at"),
        iid = m.q("invitation_id"),
        status = m.q("status"),
        expires = m.q("expires_at"),
        cols = invitation_select_clause(),
    );
    let row = sqlx::query(&sql)
        .bind(invitation_id)
        .bind(token_hash)
        .bind(STATUS_PENDING)
        .bind(MAX_SENDS)
        .bind(RESEND_MIN_INTERVAL_SECS as f64)
        .fetch_optional(pool)
        .await
        .map_err(map_err("invitation resend failed"))?;
    Ok(row.map(|row| invitation_row_from(&row)))
}

/// Revoke a PENDING invitation of `tenant_id`. `None` when it was no longer
/// PENDING.
async fn revoke_invitation_row(
    conn: &mut sqlx::PgConnection,
    invitation_id: &Uuid,
    tenant_id: &str,
    revoked_by: &str,
    reason: &str,
) -> Result<Option<InvitationRecord>, Status> {
    let m = native_model(INVITATION_MSG, &invitation_columns());
    let sql = format!(
        "UPDATE {rel} SET {status} = $3, {revision} = {revision} + 1, \
            {revoked_by} = $4, {reason} = $5, {updated} = now() \
         WHERE {iid} = $1 AND {tenant} = $2 AND {status} = $6 \
         RETURNING {cols}",
        rel = m.relation,
        status = m.q("status"),
        revision = m.q("revision"),
        revoked_by = m.q("revoked_by"),
        reason = m.q("revoke_reason"),
        updated = m.q("updated_at"),
        iid = m.q("invitation_id"),
        tenant = m.q("tenant_id"),
        cols = invitation_select_clause(),
    );
    let row = sqlx::query(&sql)
        .bind(invitation_id)
        .bind(tenant_id)
        .bind(STATUS_REVOKED)
        .bind(revoked_by)
        .bind(reason)
        .bind(STATUS_PENDING)
        .fetch_optional(&mut *conn)
        .await
        .map_err(map_err("invitation revoke failed"))?;
    Ok(row.map(|row| invitation_row_from(&row)))
}

/// Record the invitee account on an open invitation that has none yet.
/// `false` when another request recorded one first (or it closed).
async fn claim_invitee(
    conn: &mut sqlx::PgConnection,
    invitation_id: &Uuid,
    user_id: &Uuid,
) -> Result<bool, Status> {
    let m = native_model(INVITATION_MSG, &invitation_columns());
    let sql = format!(
        "UPDATE {rel} SET {invitee} = $2, {revision} = {revision} + 1, {updated} = now() \
         WHERE {iid} = $1 AND {invitee} IS NULL AND {status} = $3 AND {expires} > now()",
        rel = m.relation,
        invitee = m.q("invitee_user_id"),
        revision = m.q("revision"),
        updated = m.q("updated_at"),
        iid = m.q("invitation_id"),
        status = m.q("status"),
        expires = m.q("expires_at"),
    );
    let affected = sqlx::query(&sql)
        .bind(invitation_id)
        .bind(user_id)
        .bind(STATUS_PENDING)
        .execute(&mut *conn)
        .await
        .map_err(map_err("invitation invitee claim failed"))?
        .rows_affected();
    Ok(affected == 1)
}

/// Mark the invitation ACCEPTED by `user_id`, only while it is still open and
/// still carries the token the invitee presented (a concurrent resend or
/// revoke wins). `None` when that no longer holds.
async fn mark_invitation_accepted(
    conn: &mut sqlx::PgConnection,
    invitation_id: &Uuid,
    token_hash: &str,
    user_id: &Uuid,
) -> Result<Option<InvitationRecord>, Status> {
    let m = native_model(INVITATION_MSG, &invitation_columns());
    let sql = format!(
        "UPDATE {rel} SET {status} = $4, {accepted_user} = $3, {invitee} = $3, \
            {accepted_at} = now(), {revision} = {revision} + 1, {updated} = now() \
         WHERE {iid} = $1 AND {token_hash} = $2 AND {status} = $5 AND {expires} > now() \
         RETURNING {cols}",
        rel = m.relation,
        status = m.q("status"),
        accepted_user = m.q("accepted_user_id"),
        invitee = m.q("invitee_user_id"),
        accepted_at = m.q("accepted_at"),
        revision = m.q("revision"),
        updated = m.q("updated_at"),
        iid = m.q("invitation_id"),
        token_hash = m.q("token_hash"),
        expires = m.q("expires_at"),
        cols = invitation_select_clause(),
    );
    let row = sqlx::query(&sql)
        .bind(invitation_id)
        .bind(token_hash)
        .bind(user_id)
        .bind(STATUS_ACCEPTED)
        .bind(STATUS_PENDING)
        .fetch_optional(&mut *conn)
        .await
        .map_err(map_err("invitation accept failed"))?;
    Ok(row.map(|row| invitation_row_from(&row)))
}

/// Delete invitee accounts that never got past PENDING_VERIFICATION (their
/// invitation was revoked or replaced). An account that was activated is
/// never touched.
async fn delete_pending_invitees(
    conn: &mut sqlx::PgConnection,
    user_ids: &[String],
) -> Result<(), Status> {
    let um = native_model(USER_MSG, &["user_id", "status"]);
    let sql = format!(
        "DELETE FROM {rel} WHERE {uid} = $1 AND {status} = $2",
        rel = um.relation,
        uid = um.q("user_id"),
        status = um.q("status"),
    );
    for user_id in user_ids {
        let Ok(user_id) = Uuid::parse_str(user_id.trim()) else {
            continue;
        };
        sqlx::query(&sql)
            .bind(user_id)
            .bind(crate::runtime::authn::AccountStatus::PendingVerification.to_db())
            .execute(&mut *conn)
            .await
            .map_err(map_err("invitation invitee cleanup failed"))?;
    }
    Ok(())
}

/// Whether an account other than `except_user_id` already uses this phone in
/// the tenant.
async fn phone_in_use(
    pool: &PgPool,
    tenant_id: &str,
    phone: &str,
    except_user_id: &str,
) -> Result<bool, Status> {
    let um = native_model(USER_MSG, &["user_id", "tenant_id", "phone", "deleted_at"]);
    let sql = format!(
        "SELECT 1 FROM {rel} WHERE {tenant} = $1 AND {phone} = $2 AND {del} IS NULL \
           AND {uid}::TEXT <> $3 LIMIT 1",
        rel = um.relation,
        tenant = um.q("tenant_id"),
        phone = um.q("phone"),
        del = um.q("deleted_at"),
        uid = um.q("user_id"),
    );
    let found = sqlx::query(&sql)
        .bind(tenant_id)
        .bind(phone)
        .bind(except_user_id)
        .fetch_optional(pool)
        .await
        .map_err(map_err("invitation phone lookup failed"))?;
    Ok(found.is_some())
}

/// Record the proven phone on the account, verified now. The account row was
/// written earlier in this transaction.
async fn set_verified_phone(
    conn: &mut sqlx::PgConnection,
    user_id: &Uuid,
    phone: &str,
    now: u64,
) -> Result<(), Status> {
    let um = native_model(
        USER_MSG,
        &["user_id", "phone", "phone_verified_at", "updated_at"],
    );
    let sql = format!(
        "UPDATE {rel} SET {phone} = $2, {verified} = to_timestamp($3::DOUBLE PRECISION), \
            {updated} = now() WHERE {uid} = $1",
        rel = um.relation,
        phone = um.q("phone"),
        verified = um.q("phone_verified_at"),
        updated = um.q("updated_at"),
        uid = um.q("user_id"),
    );
    let affected = sqlx::query(&sql)
        .bind(user_id)
        .bind(phone)
        .bind(now as f64)
        .execute(&mut *conn)
        .await
        .map_err(map_err("invitation phone verification failed"))?
        .rows_affected();
    if affected != 1 {
        return Err(authn_internal_status(
            "accept_invitation_phone",
            format!("verified phone update touched {affected} rows, expected 1"),
        ));
    }
    Ok(())
}

/// Link the proven external identity to the account (the same table
/// ResolveExternalIdentity / LinkIdentity use). An identity that already
/// belongs to an account is refused.
async fn link_invited_identity(
    conn: &mut sqlx::PgConnection,
    tenant_id: &str,
    provider_id: &Uuid,
    subject: &str,
    user_id: &Uuid,
    email: &str,
    email_verified: bool,
) -> Result<(), Status> {
    let m = native_model(
        EXTERNAL_IDENTITY_MSG,
        &[
            "external_identity_id",
            "tenant_id",
            "provider_id",
            "subject",
            "user_id",
            "email",
            "email_verified",
            "last_login_at",
        ],
    );
    let sql = format!(
        "INSERT INTO {rel} ({id}, {tenant}, {pid}, {sub}, {uid}, {email}, {ev}, {lla}) \
         VALUES (gen_random_uuid(), $1, $2, $3, $4, $5, $6, now())",
        rel = m.relation,
        id = m.q("external_identity_id"),
        tenant = m.q("tenant_id"),
        pid = m.q("provider_id"),
        sub = m.q("subject"),
        uid = m.q("user_id"),
        email = m.q("email"),
        ev = m.q("email_verified"),
        lla = m.q("last_login_at"),
    );
    sqlx::query(&sql)
        .bind(tenant_id)
        .bind(provider_id)
        .bind(subject)
        .bind(user_id)
        .bind(email)
        .bind(email_verified)
        .execute(&mut *conn)
        .await
        .map_err(
            |err| match super::super::grants::unique_violation_constraint(&err) {
                Some(_) => invitation_precondition_fields(
                    "this identity-provider account already belongs to an account of the tenant",
                    [(
                        "oidc_id_token",
                        "must be an identity that is not linked to another account",
                    )],
                ),
                None => map_err("invitation identity link failed")(err),
            },
        )?;
    Ok(())
}

// ── Events ───────────────────────────────────────────────────────────────────

/// One invitation lifecycle event. The payload carries the masked address
/// only; the token never appears.
fn build_invitation_event(
    topic: &'static str,
    record: &InvitationRecord,
    operation: &str,
    reason_code: &str,
    actor: &str,
    auth_method: &str,
) -> AuthEvent {
    AuthEvent::new(
        topic,
        record.invitation_id.clone(),
        record.tenant_id.clone(),
        serde_json::json!({
            "invitation_id": record.invitation_id,
            "tenant_id": record.tenant_id,
            "project_id": record.project_id,
            "channel": record.channel,
            "masked_address": record.masked_address(),
            "idp_provider_id": record.idp_provider_id,
            "roles": record.roles,
            "status": record.status,
            "revision": record.revision,
            "expires_at_unix": record.expires_at_unix,
            "accepted_user_id": record.accepted_user_id,
            "operation": operation,
        }),
    )
    .with_correlation(format!(
        "invitation:{}:{}",
        record.invitation_id, record.revision
    ))
    .with_compliance(ComplianceEnvelope {
        actor: actor.to_string(),
        target_resource: format!("invitation:{}", record.invitation_id),
        target_tenant: record.tenant_id.clone(),
        target_project: record.project_id.clone(),
        operation: operation.to_string(),
        outcome: "success".to_string(),
        reason_code: reason_code.to_string(),
        auth_method: auth_method.to_string(),
        ..ComplianceEnvelope::default()
    })
}

// ── Authorization ────────────────────────────────────────────────────────────

/// The tenant (and project) an invitation-management call acts in: the
/// caller's own (a body naming another tenant is refused unless the caller is
/// a cross-tenant admin). Requires the manage-invitations scope (or a broad
/// admin scope). In-process calls with no claim context use the body.
fn invitation_admin_scope(
    body_tenant: &str,
    body_project: &str,
) -> Result<(String, String), Status> {
    let ctx = crate::runtime::service::method_security::current_claim_context();
    crate::runtime::service::method_security::authorize_action(
        &ctx,
        "authn.invitation.manage",
        &[MANAGE_INVITATIONS_SCOPE, "udb:authn:admin"],
    )?;
    let (tenant, project) = crate::runtime::service::method_security::resolve_body_tenant_scope(
        &ctx,
        body_tenant,
        body_project,
    )?;
    let tenant = tenant.trim().to_string();
    if tenant.is_empty() {
        return Err(invitation_invalid_fields(
            "tenant_id is required",
            [(
                "tenant_id",
                "must name the tenant the invitation belongs to",
            )],
        ));
    }
    Ok((tenant, project.trim().to_string()))
}

/// Whether the caller may manage an invitation of `tenant_id`: its own tenant,
/// or any tenant for a cross-tenant admin. A foreign invitation is reported as
/// not found by the caller, never as forbidden.
fn caller_manages_tenant(tenant_id: &str) -> Result<bool, Status> {
    if !crate::runtime::service::method_security::claim_context_present() {
        return Ok(true);
    }
    let ctx = crate::runtime::service::method_security::current_claim_context();
    crate::runtime::service::method_security::authorize_action(
        &ctx,
        "authn.invitation.manage",
        &[MANAGE_INVITATIONS_SCOPE, "udb:authn:admin"],
    )?;
    Ok(ctx.is_cross_tenant_admin()
        || (!ctx.tenant_id.trim().is_empty() && ctx.tenant_id.trim() == tenant_id.trim()))
}

fn parse_invitation_id(raw: &str) -> Result<Uuid, Status> {
    Uuid::parse_str(raw.trim()).map_err(|_| {
        invitation_invalid_fields(
            "invitation_id must be an invitation UUID",
            [("invitation_id", "must be the invitation's UUID")],
        )
    })
}

// ── Handlers ─────────────────────────────────────────────────────────────────

impl AuthnServiceImpl {
    /// The key accept tokens are hashed with (the session hash secret).
    fn invitation_token_key(&self) -> Result<Vec<u8>, Status> {
        let key = self.hash_key();
        if key.is_empty() {
            return Err(invitation_token_secret_status());
        }
        Ok(key)
    }

    /// The open invitation `invitation_id` whose stored hash matches `token`,
    /// or the ONE uniform refusal.
    async fn open_invitation(
        &self,
        invitation_id: &str,
        token: &str,
        now: u64,
    ) -> Result<InvitationRecord, Status> {
        let Ok(id) = Uuid::parse_str(invitation_id.trim()) else {
            return Err(invitation_unavailable_status());
        };
        let token = token.trim();
        if token.is_empty() {
            return Err(invitation_unavailable_status());
        }
        let key = self.invitation_token_key()?;
        let pool = self.require_pool()?;
        let record = get_invitation_by_id(pool, &id)
            .await?
            .ok_or_else(invitation_unavailable_status)?;
        let presented = authn::hash_secret(token, &key);
        if record.token_hash.is_empty()
            || !authn::constant_time_eq(&presented, &record.token_hash)
            || !record.is_open(now)
        {
            return Err(invitation_unavailable_status());
        }
        Ok(record)
    }

    /// The inviting administrator's account, when `created_by` is one.
    async fn inviter_account(&self, created_by: &str) -> Option<UserRecord> {
        Uuid::parse_str(created_by.trim()).ok()?;
        self.users
            .get_user_by_id(created_by.trim())
            .await
            .ok()
            .flatten()
    }

    /// The tenant's display name for messages and GetInvitation; empty when it
    /// cannot be read (best effort: never blocks an invitation).
    async fn tenant_display_name(&self, tenant_id: &str) -> String {
        let Some(pool) = self.pg_pool.as_ref() else {
            return String::new();
        };
        let m = native_model(TENANT_MSG, &["tenant_id", "code", "name", "deleted_at"]);
        let sql = format!(
            "SELECT COALESCE({name}, '') FROM {rel} \
             WHERE ({tid}::TEXT = $1 OR {code} = $1) AND {del} IS NULL LIMIT 1",
            name = m.q("name"),
            rel = m.relation,
            tid = m.q("tenant_id"),
            code = m.q("code"),
            del = m.q("deleted_at"),
        );
        match sqlx::query_scalar::<_, String>(&sql)
            .bind(tenant_id)
            .fetch_optional(pool)
            .await
        {
            Ok(name) => name.unwrap_or_default(),
            Err(err) => {
                tracing::warn!(error = %err, "invitation tenant name lookup failed");
                String::new()
            }
        }
    }

    /// Send the invitation message for an SMS/EMAIL invitation, post-commit
    /// and best effort (the invitation is durable; ResendInvitation retries).
    /// A configured `UDB_OTP_DELIVERY_WEBHOOK_URL` wins, as for every auth
    /// code; otherwise NotificationService renders the tenant's (or the
    /// built-in) `authn.invitation` template in the invitation's locale with
    /// `link`, `tenant_name`,
    /// `inviter_name`, `expires_in_days`, `code` (the accept token, for SMS
    /// readers who cannot open links) and `user_name`. `true` when the message
    /// was handed to a delivery channel.
    async fn send_invitation_message(
        &self,
        record: &InvitationRecord,
        token: &str,
        link: &str,
        now: u64,
    ) -> bool {
        let Some(delivery) = record
            .channel()
            .and_then(InvitationChannel::delivery_channel)
        else {
            return false;
        };
        if authn::otp_delivery_webhook_configured() {
            // The webhook carries one value: the link when there is one.
            let payload = if link.is_empty() { token } else { link };
            authn::deliver_otp(
                delivery,
                &record.address,
                payload,
                authn_entity_pb::OtpType::Invitation as i32,
                &record.invitation_id,
                (!link.is_empty()).then_some(link),
            )
            .await;
            return true;
        }
        let inviter_name = self
            .inviter_account(&record.created_by)
            .await
            .map(|user| {
                if user.full_name.trim().is_empty() {
                    user.username
                } else {
                    user.full_name
                }
            })
            .unwrap_or_default();
        let expires_in_days = record
            .expires_at_unix
            .saturating_sub(now)
            .div_ceil(24 * 60 * 60)
            .max(1);
        let variables = std::collections::HashMap::from([
            ("link".to_string(), link.to_string()),
            (
                "tenant_name".to_string(),
                self.tenant_display_name(&record.tenant_id).await,
            ),
            ("inviter_name".to_string(), inviter_name),
            ("expires_in_days".to_string(), expires_in_days.to_string()),
            ("code".to_string(), token.to_string()),
            ("user_name".to_string(), record.full_name.clone()),
        ]);
        let sent = self
            .send_localized_auth_notification(
                &record.tenant_id,
                &record.project_id,
                INVITATION_EVENT_TYPE,
                delivery,
                &record.invitation_id,
                &record.address,
                &record.locale,
                variables,
            )
            .await;
        if !sent {
            tracing::warn!(
                invitation_id = %record.invitation_id,
                "invitation message not sent: no NotificationService, no authn.invitation template for the channel, or the send failed (and no UDB_OTP_DELIVERY_WEBHOOK_URL)"
            );
        }
        sent
    }

    /// Refuse an SMS/EMAIL invitation into a tenant that requires
    /// multi-factor sign-in (a single code would admit the account).
    async fn require_channel_meets_tenant_mfa(
        &self,
        tenant_id: &str,
        channel: InvitationChannel,
    ) -> Result<(), Status> {
        if channel.delivery_channel().is_none() {
            return Ok(());
        }
        let requires_mfa = self
            .users
            .tenant_requires_mfa(tenant_id)
            .await
            .map_err(|err| authn_internal_status("invitation_tenant_mfa_policy", err))?;
        if requires_mfa {
            return Err(tenant_mfa_requires_strong_channel_status());
        }
        Ok(())
    }

    /// The invitee's account for an open invitation: the one already recorded
    /// on it, else a new PENDING_VERIFICATION account (random password, no
    /// email or phone, so nobody can sign in to it) recorded atomically. Two
    /// concurrent starts converge on whichever account was recorded first.
    async fn invitee_account(
        &self,
        record: &InvitationRecord,
        now: u64,
    ) -> Result<UserRecord, Status> {
        if !record.invitee_user_id.is_empty()
            && let Some(user) = self
                .users
                .get_user_by_id(&record.invitee_user_id)
                .await
                .map_err(|err| authn_internal_status("invitation_invitee_load", err))?
        {
            return Ok(user);
        }
        if self.password_hash_key().is_empty() {
            return Err(invitation_password_secret_status());
        }
        let invitation_uuid =
            Uuid::parse_str(&record.invitation_id).map_err(|_| invitation_unavailable_status())?;
        let user_uuid = Uuid::new_v4();
        let unusable_password = format!("{}{}", Uuid::new_v4(), Uuid::new_v4());
        let created_by = self
            .inviter_account(&record.created_by)
            .await
            .map(|inviter| inviter.user_id)
            .unwrap_or_default();
        let account = UserRecord {
            user_id: user_uuid.to_string(),
            // Placeholder handle until AcceptInvitation sets the chosen one.
            username: format!("invitee-{}", invitation_uuid.simple()),
            email: String::new(),
            password_hash: authn::password_cpu::hash(&unusable_password, &self.password_hash_key())
                .await
                .map_err(|err| authn_internal_status("invitation_invitee_password", err))?,
            account_kind: crate::runtime::authn::AccountKind::Person,
            status: crate::runtime::authn::AccountStatus::PendingVerification,
            tenant_id: record.tenant_id.clone(),
            full_name: record.full_name.clone(),
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
            project_id: record.project_id.clone(),
            external_provider_id: String::new(),
            external_subject: String::new(),
            profile_attributes_json: serde_json::json!({ "invitation_id": record.invitation_id })
                .to_string(),
            phone: String::new(),
            phone_verified_at_unix: 0,
            locale: String::new(),
        };
        let pool = self.require_pool()?;
        let mut tx = pool.begin().await.map_err(|err| {
            authn_internal_status(
                "invitation_invitee_tx_begin",
                format!("transaction begin failed: {err}"),
            )
        })?;
        self.users
            .put_user_in_tx(&mut *tx, account.clone())
            .await
            .map_err(crate::runtime::executor_utils::status_from_store_string)?;
        if claim_invitee(&mut *tx, &invitation_uuid, &user_uuid).await? {
            tx.commit().await.map_err(|err| {
                authn_internal_status(
                    "invitation_invitee_tx_commit",
                    format!("transaction commit failed: {err}"),
                )
            })?;
            return Ok(account);
        }
        // Another request recorded an account first (or the invitation closed):
        // drop ours (rollback) and use theirs.
        drop(tx);
        let current = get_invitation_by_id(pool, &invitation_uuid)
            .await?
            .filter(|current| current.is_open(now) && !current.invitee_user_id.is_empty())
            .ok_or_else(invitation_unavailable_status)?;
        self.users
            .get_user_by_id(&current.invitee_user_id)
            .await
            .map_err(|err| authn_internal_status("invitation_invitee_load", err))?
            .ok_or_else(invitation_unavailable_status)
    }

    /// Reload the shared authz snapshot after new role bindings committed, so
    /// the token issued right after acceptance already carries the roles (other
    /// nodes reload on the appended authz revision).
    async fn reload_role_bindings(&self) {
        let (Some(cell), Some(pool)) = (self.authz_snapshot.as_ref(), self.pg_pool.as_ref()) else {
            return;
        };
        super::super::AuthzServiceImpl::shared(cell.clone())
            .with_postgres(Some(pool.clone()))
            .warm_shared_snapshot()
            .await;
    }

    pub(super) async fn create_invitation_impl(
        &self,
        request: Request<authn_pb::CreateInvitationRequest>,
    ) -> Result<Response<authn_pb::CreateInvitationResponse>, Status> {
        let req = request.into_inner();
        let (tenant, project) = invitation_admin_scope(&req.tenant_id, &req.project_id)?;
        let channel = parse_channel(&req.channel)?;
        let address = normalized_address(channel, &req.address)?;
        let idp_provider_id = validated_provider(channel, &req.idp_provider_id)?;
        let roles = validated_roles(&req.roles)?;
        require_owner_authority(&roles)?;
        require_roles_outside_governed_mode(&roles, super::super::authz::governed_mode_enabled())?;
        let full_name = bounded_text(&req.full_name, "full_name", 255)?;
        let ttl = invitation_ttl_secs(req.expires_in_seconds)?;
        let locale = super::core::validated_user_locale(&req.locale)?;
        let key = self.invitation_token_key()?;
        let pool = self.require_pool()?;
        self.require_channel_meets_tenant_mfa(&tenant, channel)
            .await?;
        // An address that already has an account cannot be invited into a new
        // one (emails are unique across tenants; phones within the tenant).
        let address_taken = match channel {
            InvitationChannel::Email => self
                .users
                .get_user_by_email(&address)
                .await
                .map_err(|err| authn_internal_status("invitation_email_lookup", err))?
                .is_some(),
            InvitationChannel::Sms => phone_in_use(pool, &tenant, &address, "").await?,
            InvitationChannel::Oidc | InvitationChannel::Passkey => false,
        };
        if address_taken {
            return Err(invitation_precondition_fields(
                "the address already belongs to an account",
                [("address", "must not belong to an existing account")],
            ));
        }
        {
            let mut conn = pool
                .acquire()
                .await
                .map_err(map_err("invitation role lookup"))?;
            let (_, missing) = super::super::role_bindings::resolve_role_codes(
                &mut *conn, &tenant, &project, &roles,
            )
            .await?;
            if !missing.is_empty() {
                return Err(invitation_precondition_fields(
                    format!(
                        "unknown role code(s) in this tenant: {}",
                        missing.join(", ")
                    ),
                    [(
                        "roles",
                        "every role must be an active role code of the tenant",
                    )],
                ));
            }
        }
        let policy = self.tenant_auth_policy(&tenant).await?;
        let now = now_unix();
        let token = new_accept_token();
        let token_hash = authn::hash_secret(&token, &key);
        let created_by = caller_updated_by();
        // The invitation, the expiry of a lapsed one for the same address (and
        // its unused invitee account) and the audit event commit together.
        let mut tx = pool.begin().await.map_err(|err| {
            authn_internal_status(
                "create_invitation_tx_begin",
                format!("transaction begin failed: {err}"),
            )
        })?;
        if !address.is_empty() {
            let lapsed =
                expire_lapsed_invitations(&mut *tx, &tenant, channel.as_str(), &address).await?;
            delete_pending_invitees(&mut *tx, &lapsed).await?;
        }
        let record = insert_invitation(
            &mut *tx,
            &NewInvitation {
                tenant_id: &tenant,
                project_id: &project,
                channel,
                address: &address,
                idp_provider_id: &idp_provider_id,
                roles: &roles,
                full_name: &full_name,
                token_hash: &token_hash,
                expires_at_unix: now.saturating_add(ttl),
                created_by: &created_by,
                locale: &locale,
            },
        )
        .await?;
        self.emit_event_in_tx(
            &mut *tx,
            build_invitation_event(
                TOPIC_INVITATION_CREATED,
                &record,
                "create",
                "invitation_created",
                &created_by,
                &event_auth_method(),
            ),
        )
        .await?;
        tx.commit().await.map_err(|err| {
            authn_internal_status(
                "create_invitation_tx_commit",
                format!("transaction commit failed: {err}"),
            )
        })?;
        let accept_link =
            invitation_link(&policy.magic_link_base_url, &record.invitation_id, &token);
        let delivered = self
            .send_invitation_message(&record, &token, &accept_link, now)
            .await;
        Ok(Response::new(authn_pb::CreateInvitationResponse {
            invitation: Some(record.to_info(now)),
            accept_token: token,
            accept_link,
            delivered,
        }))
    }

    pub(super) async fn list_invitations_impl(
        &self,
        request: Request<authn_pb::ListInvitationsRequest>,
    ) -> Result<Response<authn_pb::ListInvitationsResponse>, Status> {
        let req = request.into_inner();
        let (tenant, _) = invitation_admin_scope(&req.tenant_id, "")?;
        let status = validated_status_filter(&req.status)?;
        let pool = self.require_pool()?;
        let (limit, offset) = page_window(req.page_size, &req.page_token);
        let records = list_invitations(pool, &tenant, &status, limit, offset).await?;
        let now = now_unix();
        Ok(Response::new(authn_pb::ListInvitationsResponse {
            next_page_token: next_page_token(records.len(), limit, offset),
            invitations: records.iter().map(|record| record.to_info(now)).collect(),
        }))
    }

    pub(super) async fn revoke_invitation_impl(
        &self,
        request: Request<authn_pb::RevokeInvitationRequest>,
    ) -> Result<Response<authn_pb::RevokeInvitationResponse>, Status> {
        let req = request.into_inner();
        let invitation_id = parse_invitation_id(&req.invitation_id)?;
        let reason = bounded_text(&req.reason, "reason", 1024)?;
        let pool = self.require_pool()?;
        let existing = get_invitation_by_id(pool, &invitation_id)
            .await?
            .ok_or_else(invitation_not_found_status)?;
        if !caller_manages_tenant(&existing.tenant_id)? {
            return Err(invitation_not_found_status());
        }
        let revoked_by = caller_updated_by();
        // The revocation, the deletion of the unused invitee account and the
        // audit event commit together.
        let mut tx = pool.begin().await.map_err(|err| {
            authn_internal_status(
                "revoke_invitation_tx_begin",
                format!("transaction begin failed: {err}"),
            )
        })?;
        let revoked = revoke_invitation_row(
            &mut *tx,
            &invitation_id,
            &existing.tenant_id,
            &revoked_by,
            &reason,
        )
        .await?;
        if let Some(record) = &revoked {
            delete_pending_invitees(&mut *tx, std::slice::from_ref(&record.invitee_user_id))
                .await?;
            self.emit_event_in_tx(
                &mut *tx,
                build_invitation_event(
                    TOPIC_INVITATION_REVOKED,
                    record,
                    "revoke",
                    "invitation_revoked",
                    &revoked_by,
                    &event_auth_method(),
                ),
            )
            .await?;
        }
        tx.commit().await.map_err(|err| {
            authn_internal_status(
                "revoke_invitation_tx_commit",
                format!("transaction commit failed: {err}"),
            )
        })?;
        let now = now_unix();
        Ok(Response::new(authn_pb::RevokeInvitationResponse {
            revoked: revoked.is_some(),
            invitation: Some(revoked.unwrap_or(existing).to_info(now)),
        }))
    }

    pub(super) async fn resend_invitation_impl(
        &self,
        request: Request<authn_pb::ResendInvitationRequest>,
    ) -> Result<Response<authn_pb::ResendInvitationResponse>, Status> {
        let req = request.into_inner();
        let invitation_id = parse_invitation_id(&req.invitation_id)?;
        let key = self.invitation_token_key()?;
        let pool = self.require_pool()?;
        let existing = get_invitation_by_id(pool, &invitation_id)
            .await?
            .ok_or_else(invitation_not_found_status)?;
        if !caller_manages_tenant(&existing.tenant_id)? {
            return Err(invitation_not_found_status());
        }
        if existing
            .channel()
            .and_then(InvitationChannel::delivery_channel)
            .is_none()
        {
            return Err(invitation_precondition_fields(
                "only SMS and EMAIL invitations are sent; hand an OIDC or PASSKEY link over yourself",
                [("invitation_id", "must be an SMS or EMAIL invitation")],
            ));
        }
        let now = now_unix();
        let throttle = |record: &InvitationRecord| -> Status {
            if !record.is_open(now) {
                return invitation_precondition_fields(
                    "only a PENDING, unexpired invitation can be resent",
                    [("invitation_id", "must be a PENDING, unexpired invitation")],
                );
            }
            if record.send_count >= MAX_SENDS {
                return send_limit_reached_status();
            }
            resend_throttled_status(
                record
                    .last_sent_at_unix
                    .saturating_add(RESEND_MIN_INTERVAL_SECS),
                now,
            )
        };
        let token = new_accept_token();
        let Some(record) =
            record_resend(pool, &invitation_id, &authn::hash_secret(&token, &key)).await?
        else {
            // Closed, throttled, or a concurrent resend won: report why.
            let current = get_invitation_by_id(pool, &invitation_id)
                .await?
                .unwrap_or(existing);
            return Err(throttle(&current));
        };
        let actor = caller_updated_by();
        self.emit_event(build_invitation_event(
            TOPIC_INVITATION_RESENT,
            &record,
            "resend",
            "invitation_resent",
            &actor,
            &event_auth_method(),
        ))
        .await;
        let policy = self.tenant_auth_policy(&record.tenant_id).await?;
        let accept_link =
            invitation_link(&policy.magic_link_base_url, &record.invitation_id, &token);
        let delivered = self
            .send_invitation_message(&record, &token, &accept_link, now)
            .await;
        Ok(Response::new(authn_pb::ResendInvitationResponse {
            invitation: Some(record.to_info(now)),
            accept_token: token,
            accept_link,
            delivered,
        }))
    }

    pub(super) async fn get_invitation_impl(
        &self,
        request: Request<authn_pb::GetInvitationRequest>,
    ) -> Result<Response<authn_pb::GetInvitationResponse>, Status> {
        let req = request.into_inner();
        let now = now_unix();
        let record = self
            .open_invitation(&req.invitation_id, &req.token, now)
            .await?;
        let channel = record.channel().ok_or_else(invitation_unavailable_status)?;
        let policy = self.tenant_auth_policy(&record.tenant_id).await?;
        Ok(Response::new(authn_pb::GetInvitationResponse {
            tenant_name: self.tenant_display_name(&record.tenant_id).await,
            channel: channel.as_str().to_string(),
            roles: record.roles.clone(),
            expires_at: timestamp_from_unix(record.expires_at_unix),
            masked_address: masked_address(channel, &record.address),
            idp_provider_id: record.idp_provider_id.clone(),
            password_required: policy.password_required,
            invitation_id: record.invitation_id,
        }))
    }

    pub(super) async fn start_invitation_acceptance_impl(
        &self,
        request: Request<authn_pb::StartInvitationAcceptanceRequest>,
    ) -> Result<Response<authn_pb::StartInvitationAcceptanceResponse>, Status> {
        let req = request.into_inner();
        let now = now_unix();
        let record = self
            .open_invitation(&req.invitation_id, &req.token, now)
            .await?;
        let channel = record.channel().ok_or_else(invitation_unavailable_status)?;
        // Governed mode switched on after the invitation was made: its roles
        // cannot be bound, so do not send a code that could never complete.
        require_roles_outside_governed_mode(
            &record.roles,
            super::super::authz::governed_mode_enabled(),
        )?;
        match channel {
            InvitationChannel::Sms | InvitationChannel::Email => {
                let delivery = channel.delivery_channel().unwrap_or("email");
                let account = self.invitee_account(&record, now).await?;
                let otp_type = authn_entity_pb::OtpType::Invitation as i32;
                // The OTP cooldown (per account and code type) throttles
                // repeated starts.
                let (otp_id, code) = self
                    .issue_otp_to(
                        &account,
                        otp_type,
                        delivery,
                        &record.address,
                        format!("invitation:{}", record.invitation_id),
                        now,
                    )
                    .await?;
                self.emit_event(
                    AuthEvent::new(
                        topics::OTP_SENT,
                        otp_id.clone(),
                        record.tenant_id.clone(),
                        serde_json::json!({
                            "otp_id": otp_id.clone(),
                            "user_id": account.user_id.clone(),
                            "otp_type": otp_type,
                            "tenant_id": record.tenant_id.clone(),
                            "invitation_id": record.invitation_id.clone(),
                        }),
                    )
                    .with_correlation(format!("otp_sent:{otp_id}"))
                    .with_compliance(ComplianceEnvelope {
                        actor: account.user_id.clone(),
                        target_resource: format!("invitation:{}", record.invitation_id),
                        operation: "otp_send".to_string(),
                        outcome: "success".to_string(),
                        reason_code: "invitation_code_issued".to_string(),
                        auth_method: "invitation".to_string(),
                        ..ComplianceEnvelope::default()
                    }),
                )
                .await;
                let dev_otp_code = if super::mfa::otp_dev_echo_enabled() {
                    code
                } else {
                    String::new()
                };
                Ok(Response::new(authn_pb::StartInvitationAcceptanceResponse {
                    otp_id,
                    otp_expires_at_unix: now.saturating_add(self.config.otp_ttl_secs) as i64,
                    dev_otp_code,
                    ..Default::default()
                }))
            }
            InvitationChannel::Passkey => {
                let account = self.invitee_account(&record, now).await?;
                let (challenge_id, options_json, _expires_at_unix) = self
                    .register_passkey_for_new_user_start(&account, INVITATION_PASSKEY_LABEL)
                    .await?;
                Ok(Response::new(authn_pb::StartInvitationAcceptanceResponse {
                    webauthn_challenge_id: challenge_id,
                    webauthn_options_json: options_json,
                    ..Default::default()
                }))
            }
            InvitationChannel::Oidc => {
                if req.redirect_uri.trim().is_empty() {
                    return Err(invitation_invalid_fields(
                        "redirect_uri is required to start an OIDC invitation",
                        [(
                            "redirect_uri",
                            "must be a redirect URI allowlisted for the identity provider",
                        )],
                    ));
                }
                // The sign-in is bound to this invitation: only
                // AcceptInvitation can redeem its callback, never
                // FinishOidcLogin.
                let started = self
                    .start_oidc_authorization(
                        &record.tenant_id,
                        &record.idp_provider_id,
                        req.redirect_uri.trim(),
                        &record.invitation_id,
                        "",
                    )
                    .await?;
                Ok(Response::new(authn_pb::StartInvitationAcceptanceResponse {
                    authorization_url: started.authorization_url,
                    oidc_state: started.state,
                    ..Default::default()
                }))
            }
        }
    }

    pub(super) async fn accept_invitation_impl(
        &self,
        request: Request<authn_pb::AcceptInvitationRequest>,
    ) -> Result<Response<authn_pb::AcceptInvitationResponse>, Status> {
        let req = request.into_inner();
        let now = now_unix();
        let record = self
            .open_invitation(&req.invitation_id, &req.token, now)
            .await?;
        let channel = record.channel().ok_or_else(invitation_unavailable_status)?;
        // Re-checked here: governed mode may have been switched on since the
        // invitation was made. Nothing is bound or spent and the invitation
        // stays PENDING.
        require_roles_outside_governed_mode(
            &record.roles,
            super::super::authz::governed_mode_enabled(),
        )?;
        let username = validated_username(&req.username)?;
        let full_name = if req.full_name.trim().is_empty() {
            record.full_name.clone()
        } else {
            bounded_text(&req.full_name, "full_name", 255)?
        };
        let policy = self.tenant_auth_policy(&record.tenant_id).await?;
        let password = req.password.as_str();
        if password.is_empty() {
            if policy.password_required {
                return Err(invitation_invalid_fields(
                    "this tenant requires a password",
                    [("password", "must be set: the tenant requires a password")],
                ));
            }
        } else {
            authn::PasswordPolicy::from_env()
                .validate(password)
                .map_err(|message| {
                    invitation_invalid_fields(
                        message,
                        [(
                            "password",
                            "must satisfy the configured password policy for native users",
                        )],
                    )
                })?;
        }
        if self.password_hash_key().is_empty() {
            return Err(invitation_password_secret_status());
        }
        self.require_channel_meets_tenant_mfa(&record.tenant_id, channel)
            .await?;
        // The chosen username must be free (the invitee account's placeholder
        // is the account itself). Checked before the proof is spent.
        if let Some(existing) = self
            .users
            .get_user_by_username(&username)
            .await
            .map_err(|err| authn_internal_status("accept_invitation_username", err))?
            && existing.user_id != record.invitee_user_id
        {
            return Err(username_taken_status());
        }

        // ── Proof of the channel ─────────────────────────────────────────────
        let mut linked_identity = None;
        let mut account = match channel {
            InvitationChannel::Sms | InvitationChannel::Email => {
                if req.otp_id.trim().is_empty() || req.code.trim().is_empty() {
                    return Err(invitation_invalid_fields(
                        "otp_id and code are required to accept an SMS or EMAIL invitation",
                        [
                            (
                                "otp_id",
                                "must be the otp_id from StartInvitationAcceptance",
                            ),
                            ("code", "must be the code sent to the invitation address"),
                        ],
                    ));
                }
                let otp = self
                    .verify_otp_record(
                        req.otp_id.trim(),
                        req.code.trim(),
                        Some(authn_entity_pb::OtpType::Invitation as i32),
                        now,
                    )
                    .await?
                    .ok_or_else(invalid_invitation_code_status)?;
                // The code must have been sent for THIS invitation's account,
                // to THIS invitation's address.
                if record.invitee_user_id.is_empty()
                    || otp.user_id != record.invitee_user_id
                    || !otp.delivery_address.eq_ignore_ascii_case(&record.address)
                {
                    return Err(invalid_invitation_code_status());
                }
                self.users
                    .get_user_by_id(&record.invitee_user_id)
                    .await
                    .map_err(|err| authn_internal_status("accept_invitation_account", err))?
                    .ok_or_else(invitation_unavailable_status)?
            }
            InvitationChannel::Passkey => {
                if req.webauthn_challenge_id.trim().is_empty()
                    || req.webauthn_attestation_json.trim().is_empty()
                {
                    return Err(invitation_invalid_fields(
                        "webauthn_challenge_id and webauthn_attestation_json are required to accept a PASSKEY invitation",
                        [
                            (
                                "webauthn_challenge_id",
                                "must be the challenge from StartInvitationAcceptance",
                            ),
                            (
                                "webauthn_attestation_json",
                                "must be the navigator.credentials.create() result as JSON",
                            ),
                        ],
                    ));
                }
                if record.invitee_user_id.is_empty() {
                    return Err(invitation_unavailable_status());
                }
                let account = self
                    .users
                    .get_user_by_id(&record.invitee_user_id)
                    .await
                    .map_err(|err| authn_internal_status("accept_invitation_account", err))?
                    .ok_or_else(invitation_unavailable_status)?;
                self.register_passkey_for_new_user_finish(
                    &account,
                    req.webauthn_challenge_id.trim(),
                    req.webauthn_attestation_json.trim(),
                    INVITATION_PASSKEY_LABEL,
                )
                .await?;
                account
            }
            InvitationChannel::Oidc => {
                let redirected =
                    !req.oidc_state.trim().is_empty() && !req.oidc_code.trim().is_empty();
                let identity = if redirected {
                    // The browser came back from the provider: redeem the
                    // sign-in StartInvitationAcceptance bound to this
                    // invitation (single use).
                    let (tenant_id, identity) = self
                        .redeem_oidc_invitation_code(
                            &record.invitation_id,
                            req.oidc_state.trim(),
                            req.oidc_code.trim(),
                        )
                        .await?;
                    if tenant_id.trim() != record.tenant_id.trim() {
                        return Err(invalid_invitation_identity_status());
                    }
                    identity
                } else if !req.oidc_id_token.trim().is_empty() {
                    // A native app signed in with the provider's SDK.
                    self.verify_oidc_id_token_for_tenant(
                        &record.tenant_id,
                        &record.idp_provider_id,
                        req.oidc_id_token.trim(),
                        req.oidc_nonce.trim(),
                    )
                    .await?
                } else {
                    return Err(invitation_invalid_fields(
                        "an OIDC invitation is accepted with oidc_state + oidc_code or with oidc_id_token",
                        [
                            (
                                "oidc_code",
                                "must be the provider callback code (with oidc_state)",
                            ),
                            (
                                "oidc_id_token",
                                "or an ID token from the invitation's identity provider",
                            ),
                        ],
                    ));
                };
                if identity.subject.trim().is_empty()
                    || !identity
                        .provider_id
                        .trim()
                        .eq_ignore_ascii_case(record.idp_provider_id.trim())
                {
                    return Err(invalid_invitation_identity_status());
                }
                let email = identity.email.trim().to_ascii_lowercase();
                // A verified provider email becomes the account's email unless
                // another account already uses it.
                let email = if identity.email_verified
                    && is_plausible_email(&email)
                    && self
                        .users
                        .get_user_by_email(&email)
                        .await
                        .map_err(|err| authn_internal_status("accept_invitation_email", err))?
                        .is_none()
                {
                    email
                } else {
                    String::new()
                };
                linked_identity = Some((
                    identity.subject.trim().to_string(),
                    email.clone(),
                    identity.email_verified,
                ));
                let mut account = if record.invitee_user_id.is_empty() {
                    self.invitee_account(&record, now).await?
                } else {
                    self.users
                        .get_user_by_id(&record.invitee_user_id)
                        .await
                        .map_err(|err| authn_internal_status("accept_invitation_account", err))?
                        .ok_or_else(invitation_unavailable_status)?
                };
                account.external_provider_id = record.idp_provider_id.clone();
                account.external_subject = identity.subject.trim().to_string();
                if !email.is_empty() {
                    account.email = email;
                    account.email_verified_at_unix = now;
                }
                account
            }
        };

        // ── The ACTIVE account ───────────────────────────────────────────────
        let password_secret = if password.is_empty() {
            // Sign-in by code, passkey or identity provider only: a random
            // secret nobody knows.
            format!("{}{}", Uuid::new_v4(), Uuid::new_v4())
        } else {
            password.to_string()
        };
        account.username = username;
        account.full_name = full_name;
        account.password_hash =
            authn::password_cpu::hash(&password_secret, &self.password_hash_key())
                .await
                .map_err(|err| authn_internal_status("accept_invitation_password_hash", err))?;
        account.status = crate::runtime::authn::AccountStatus::Active;
        account.account_kind = crate::runtime::authn::AccountKind::Person;
        account.tenant_id = record.tenant_id.clone();
        account.project_id = record.project_id.clone();
        account.failed_login_count = 0;
        account.locked_until_unix = 0;
        account.updated_at_unix = now;
        if channel == InvitationChannel::Email {
            account.email = record.address.clone();
            account.email_verified_at_unix = now;
        }
        // A passkey enrolled at acceptance is a second factor, exactly as
        // after StartWebAuthnRegistration.
        if channel == InvitationChannel::Passkey {
            account.mfa_enabled = true;
        }
        let user_uuid = Uuid::parse_str(&account.user_id).map_err(|_| {
            authn_internal_status(
                "accept_invitation_account",
                "invitee account id is not a UUID",
            )
        })?;
        let invitation_uuid =
            Uuid::parse_str(&record.invitation_id).map_err(|_| invitation_unavailable_status())?;
        // The inviter is the assigner of record; an inviter without an account
        // id (a service caller) leaves the account itself as assigner, as the
        // bootstrap binding does.
        let assigned_by = Uuid::parse_str(record.created_by.trim()).unwrap_or(user_uuid);
        let pool = self.require_pool()?;

        // ── One transaction: account, channel, roles, invitation, events ──────
        let mut tx = pool.begin().await.map_err(|err| {
            authn_internal_status(
                "accept_invitation_tx_begin",
                format!("transaction begin failed: {err}"),
            )
        })?;
        self.users
            .put_user_in_tx(&mut *tx, account.clone())
            .await
            .map_err(crate::runtime::executor_utils::status_from_store_string)?;
        if !record.locale.is_empty() {
            super::core::set_user_locale_in_tx(&mut *tx, &user_uuid, &record.locale).await?;
        }
        if channel == InvitationChannel::Sms {
            set_verified_phone(&mut *tx, &user_uuid, &record.address, now).await?;
            account.phone = record.address.clone();
            account.phone_verified_at_unix = now;
        }
        if let Some((subject, email, email_verified)) = &linked_identity {
            let provider = Uuid::parse_str(record.idp_provider_id.trim())
                .map_err(|_| invalid_invitation_identity_status())?;
            link_invited_identity(
                &mut *tx,
                &record.tenant_id,
                &provider,
                subject,
                &user_uuid,
                email,
                *email_verified,
            )
            .await?;
        }
        let (bound_roles, missing_roles) = super::super::role_bindings::resolve_role_codes(
            &mut *tx,
            &record.tenant_id,
            &record.project_id,
            &record.roles,
        )
        .await?;
        if !missing_roles.is_empty() {
            // Deleted or deactivated since the invitation was made: the
            // account gets fewer privileges, never different ones.
            tracing::warn!(
                invitation_id = %record.invitation_id,
                roles = ?missing_roles,
                "invited roles no longer exist in the tenant; not bound"
            );
        }
        if !bound_roles.is_empty() {
            super::super::role_bindings::bind_role_ids(
                &mut *tx,
                &user_uuid,
                &record.tenant_id,
                &bound_roles,
                &assigned_by,
                &assigned_by.to_string(),
                super::super::role_bindings::ExistingBinding::Refresh,
            )
            .await?;
            super::super::role_bindings::append_role_assignment_revision(
                &mut *tx,
                &record.tenant_id,
                &record.project_id,
                &assigned_by.to_string(),
            )
            .await?;
        }
        let Some(accepted) =
            mark_invitation_accepted(&mut *tx, &invitation_uuid, &record.token_hash, &user_uuid)
                .await?
        else {
            // Revoked, resent or accepted concurrently: nothing is kept.
            return Err(invitation_unavailable_status());
        };
        self.emit_event_in_tx(
            &mut *tx,
            AuthEvent::new(
                topics::USER_REGISTERED,
                account.user_id.clone(),
                account.tenant_id.clone(),
                serde_json::json!({
                    "user_id": account.user_id.clone(),
                    "username": account.username.clone(),
                    "email": account.email.clone(),
                    "tenant_id": account.tenant_id.clone(),
                    "project_id": account.project_id.clone(),
                    "account_kind": account_kind_to_proto(account.account_kind),
                    "created_by": account.created_by.clone(),
                    "invitation_id": accepted.invitation_id.clone(),
                }),
            )
            .with_correlation(format!("accept_invitation:{}", accepted.invitation_id))
            .with_compliance(ComplianceEnvelope {
                actor: account.user_id.clone(),
                target_resource: account.user_id.clone(),
                target_tenant: account.tenant_id.clone(),
                target_project: account.project_id.clone(),
                operation: "create".to_string(),
                outcome: "success".to_string(),
                reason_code: "invitation_accepted".to_string(),
                auth_method: "invitation".to_string(),
                ..ComplianceEnvelope::default()
            }),
        )
        .await?;
        for (role_code, role_id) in &bound_roles {
            self.emit_event_in_tx(
                &mut *tx,
                AuthEvent::new(
                    topics::ROLE_ASSIGNED,
                    account.user_id.clone(),
                    account.tenant_id.clone(),
                    serde_json::json!({
                        "user_id": account.user_id.clone(),
                        "role_id": role_id.to_string(),
                        "role_code": role_code,
                        "tenant_id": account.tenant_id.clone(),
                        "assigned_by": assigned_by.to_string(),
                        "invitation_id": accepted.invitation_id.clone(),
                    }),
                )
                .with_correlation(format!("role_assign:{}:{role_id}", account.user_id))
                .with_compliance(ComplianceEnvelope {
                    actor: accepted.created_by.clone(),
                    actor_project: account.project_id.clone(),
                    target_resource: account.user_id.clone(),
                    operation: "role_assign".to_string(),
                    outcome: "success".to_string(),
                    reason_code: "invitation_role_bound".to_string(),
                    auth_method: "invitation".to_string(),
                    ..ComplianceEnvelope::default()
                }),
            )
            .await?;
        }
        self.emit_event_in_tx(
            &mut *tx,
            build_invitation_event(
                TOPIC_INVITATION_ACCEPTED,
                &accepted,
                "accept",
                "invitation_accepted",
                &account.user_id,
                "invitation",
            ),
        )
        .await?;
        tx.commit().await.map_err(|err| {
            authn_internal_status(
                "accept_invitation_tx_commit",
                format!("transaction commit failed: {err}"),
            )
        })?;
        if !bound_roles.is_empty() {
            self.reload_role_bindings().await;
        }

        // ── Signed in, exactly like every other sign-in path ─────────────────
        let signed_in = self
            .complete_sign_in(
                account,
                super::login::SignIn {
                    device_name: &req.device_name,
                    ip_address: &req.ip_address,
                    user_agent: &req.user_agent,
                    device_id: &req.device_id,
                    requested_scopes: &[],
                    token_auth_method: "invitation",
                    audit_auth_method: "invitation",
                    audit_reason_code: "invitation_accepted",
                    restore_on_failure: None,
                },
                now,
            )
            .await?;
        Ok(Response::new(accept_invitation_response(signed_in)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::ErrorDetail;
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;
    use crate::runtime::service::method_security::{
        scope_claim_context_for_test, test_claim_context,
    };

    fn svc() -> AuthnServiceImpl {
        AuthnServiceImpl::new(AuthnConfig::default(), SecurityConfig::default())
    }

    fn strings(items: &[&str]) -> Vec<String> {
        items.iter().map(|item| item.to_string()).collect()
    }

    fn decode_detail(status: &Status) -> ErrorDetail {
        let raw = status
            .metadata()
            .get_bin(ERROR_DETAIL_METADATA_KEY)
            .expect("typed detail trailer is present")
            .to_bytes()
            .expect("typed detail trailer decodes to bytes");
        crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
    }

    fn violated_field(status: &Status) -> String {
        decode_detail(status)
            .field_violations
            .first()
            .map(|violation| violation.field.clone())
            .unwrap_or_default()
    }

    #[test]
    fn channels_parse_case_insensitively() {
        for (raw, channel) in [
            ("sms", InvitationChannel::Sms),
            (" EMAIL ", InvitationChannel::Email),
            ("Oidc", InvitationChannel::Oidc),
            ("passkey", InvitationChannel::Passkey),
        ] {
            assert_eq!(parse_channel(raw).unwrap(), channel, "{raw:?}");
            assert_eq!(InvitationChannel::parse(channel.as_str()), Some(channel));
        }
        for raw in ["", "phone", "magic_link"] {
            let err = parse_channel(raw).unwrap_err();
            assert_eq!(err.code(), tonic::Code::InvalidArgument);
            assert_eq!(violated_field(&err), "channel");
        }
        assert_eq!(InvitationChannel::Sms.delivery_channel(), Some("sms"));
        assert_eq!(InvitationChannel::Email.delivery_channel(), Some("email"));
        assert_eq!(InvitationChannel::Oidc.delivery_channel(), None);
        assert_eq!(InvitationChannel::Passkey.delivery_channel(), None);
    }

    #[test]
    fn addresses_are_validated_per_channel() {
        assert_eq!(
            normalized_address(InvitationChannel::Sms, " +8801712345678 ").unwrap(),
            "+8801712345678"
        );
        assert_eq!(
            normalized_address(InvitationChannel::Email, " Rina@Example.COM ").unwrap(),
            "rina@example.com"
        );
        assert_eq!(
            normalized_address(InvitationChannel::Passkey, "").unwrap(),
            ""
        );
        for (channel, raw) in [
            (InvitationChannel::Sms, "01712345678"),
            (InvitationChannel::Sms, "+0123456789"),
            (InvitationChannel::Email, "not-an-email"),
            (InvitationChannel::Email, "a@b"),
            (InvitationChannel::Email, "a b@example.com"),
            (InvitationChannel::Oidc, "rina@example.com"),
            (InvitationChannel::Passkey, "+8801712345678"),
        ] {
            let err = normalized_address(channel, raw).unwrap_err();
            assert_eq!(
                err.code(),
                tonic::Code::InvalidArgument,
                "{channel:?} {raw:?}"
            );
            assert_eq!(violated_field(&err), "address");
        }
    }

    #[test]
    fn only_oidc_invitations_name_a_provider() {
        let provider = Uuid::new_v4().to_string();
        assert_eq!(
            validated_provider(InvitationChannel::Oidc, &provider).unwrap(),
            provider
        );
        assert_eq!(validated_provider(InvitationChannel::Sms, "").unwrap(), "");
        for (channel, raw) in [
            (InvitationChannel::Oidc, ""),
            (InvitationChannel::Oidc, "google"),
            (InvitationChannel::Email, provider.as_str()),
        ] {
            let err = validated_provider(channel, raw).unwrap_err();
            assert_eq!(
                violated_field(&err),
                "idp_provider_id",
                "{channel:?} {raw:?}"
            );
        }
    }

    #[test]
    fn roles_are_codes_deduplicated_and_never_platform_roles() {
        assert_eq!(
            validated_roles(&strings(&[" nurse ", "doctor", "NURSE"])).unwrap(),
            strings(&["nurse", "doctor"])
        );
        assert!(validated_roles(&[]).unwrap().is_empty());
        for bad in [&[""][..], &["two words"][..]] {
            let err = validated_roles(&strings(bad)).unwrap_err();
            assert_eq!(err.code(), tonic::Code::InvalidArgument, "{bad:?}");
            assert_eq!(violated_field(&err), "roles");
        }
        let too_many: Vec<String> = (0..=MAX_ROLES).map(|i| format!("role{i}")).collect();
        assert_eq!(
            validated_roles(&too_many).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
        for reserved in ["platform_admin", "udb:platform_admin"] {
            let err = validated_roles(&strings(&["nurse", reserved])).unwrap_err();
            assert_eq!(err.code(), tonic::Code::PermissionDenied, "{reserved}");
            assert_eq!(
                decode_detail(&err).policy_decision_id,
                "reserved_platform_role_authority_required"
            );
        }
    }

    #[tokio::test]
    async fn only_an_owner_or_cross_tenant_admin_may_invite_an_owner() {
        let owner = strings(&["organization_owner"]);
        // In-process (no claim context): trusted.
        require_owner_authority(&owner).expect("in-process caller is trusted");
        let plain_admin = test_claim_context(
            "admin",
            "acme",
            "",
            &[MANAGE_INVITATIONS_SCOPE],
            &["tenant_admin"],
        );
        let err = scope_claim_context_for_test(plain_admin.clone(), async {
            require_owner_authority(&owner)
        })
        .await
        .expect_err("an admin without organization_owner cannot invite one");
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
        assert_eq!(
            decode_detail(&err).policy_decision_id,
            "organization_owner_authority_required"
        );
        // Other roles need no owner authority.
        scope_claim_context_for_test(plain_admin, async {
            require_owner_authority(&strings(&["nurse"]))
        })
        .await
        .expect("non-owner roles are fine");
        let owner_admin = test_claim_context(
            "owner",
            "acme",
            "",
            &[MANAGE_INVITATIONS_SCOPE],
            &["Organization_Owner"],
        );
        scope_claim_context_for_test(owner_admin, async { require_owner_authority(&owner) })
            .await
            .expect("an owner may invite an owner");
    }

    #[test]
    fn governed_mode_refuses_invitation_roles_but_not_role_less_invitations() {
        let roles = strings(&["nurse"]);
        require_roles_outside_governed_mode(&roles, false).expect("ungoverned: roles allowed");
        require_roles_outside_governed_mode(&[], true).expect("governed: no roles is fine");
        let err = require_roles_outside_governed_mode(&roles, true)
            .expect_err("governed: roles need the approval flow");
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert_eq!(
            decode_detail(&err).policy_decision_id,
            "invitation_roles_require_governed_approval"
        );
        assert!(err.message().contains("AssignRole"));
    }

    #[test]
    fn locales_are_bcp47_shaped() {
        assert_eq!(super::core::validated_user_locale("").unwrap(), "");
        assert_eq!(super::core::validated_user_locale(" bn ").unwrap(), "bn");
        assert_eq!(
            super::core::validated_user_locale("en-US").unwrap(),
            "en-US"
        );
        for bad in ["en_US", "-bn", "bn-", "b n", &"a".repeat(36)] {
            assert_eq!(
                violated_field(&super::core::validated_user_locale(bad).unwrap_err()),
                "locale",
                "{bad:?}"
            );
        }
    }

    #[test]
    fn ttl_defaults_to_a_week_and_is_bounded() {
        assert_eq!(invitation_ttl_secs(0).unwrap(), DEFAULT_TTL_SECS);
        assert_eq!(invitation_ttl_secs(300).unwrap(), 300);
        assert_eq!(invitation_ttl_secs(2_592_000).unwrap(), MAX_TTL_SECS);
        for bad in [-1, 1, 299, 2_592_001] {
            let err = invitation_ttl_secs(bad).unwrap_err();
            assert_eq!(violated_field(&err), "expires_in_seconds", "{bad}");
        }
    }

    #[test]
    fn usernames_follow_the_users_table_rule() {
        assert_eq!(
            validated_username(" Rina.Akter_1 ").unwrap(),
            "rina.akter_1"
        );
        for bad in ["", "ab", "1rina", "rina akter", "রিনা", &"a".repeat(81)] {
            let err = validated_username(bad).unwrap_err();
            assert_eq!(violated_field(&err), "username", "{bad:?}");
        }
    }

    #[test]
    fn addresses_are_masked_for_the_invitee() {
        assert_eq!(
            masked_address(InvitationChannel::Sms, "+8801712345678"),
            "+88********678"
        );
        assert_eq!(
            masked_address(InvitationChannel::Email, "rina@example.com"),
            "r***@example.com"
        );
        assert_eq!(masked_address(InvitationChannel::Passkey, ""), "");
        assert_eq!(masked_address(InvitationChannel::Sms, "+12345"), "******");
    }

    #[test]
    fn links_use_the_tenant_base_and_tokens_are_random() {
        let token = new_accept_token();
        assert!(token.starts_with(ACCEPT_TOKEN_PREFIX));
        assert_eq!(token.len(), ACCEPT_TOKEN_PREFIX.len() + 64);
        assert_ne!(token, new_accept_token());
        assert_eq!(
            invitation_link("https://app.example.com/auth/", "id-1", &token),
            format!("https://app.example.com/auth/invite?invitation_id=id-1&token={token}")
        );
        assert_eq!(invitation_link("  ", "id-1", &token), "");
    }

    #[test]
    fn a_pending_invitation_past_expiry_reads_expired_and_is_closed() {
        let now = 1_800_000_000;
        let record = InvitationRecord {
            status: STATUS_PENDING.to_string(),
            expires_at_unix: now + 1,
            ..InvitationRecord::default()
        };
        assert!(record.is_open(now));
        assert_eq!(record.reported_status(now), STATUS_PENDING);
        assert!(!record.is_open(now + 1));
        assert_eq!(record.reported_status(now + 1), STATUS_EXPIRED);
        for status in [STATUS_ACCEPTED, STATUS_REVOKED, STATUS_EXPIRED, ""] {
            let closed = InvitationRecord {
                status: status.to_string(),
                ..record.clone()
            };
            assert!(!closed.is_open(now), "{status:?}");
        }
    }

    #[test]
    fn status_filter_accepts_the_four_states() {
        assert_eq!(validated_status_filter("").unwrap(), "");
        assert_eq!(validated_status_filter("pending").unwrap(), STATUS_PENDING);
        assert_eq!(
            violated_field(&validated_status_filter("open").unwrap_err()),
            "status"
        );
    }

    #[test]
    fn accept_response_maps_every_login_field() {
        let signed_in = authn_pb::LoginResponse {
            user_id: "u".into(),
            session_id: "s".into(),
            access_token: "a".into(),
            refresh_token: "r".into(),
            access_token_expires_in: 1,
            session_token: "st".into(),
            csrf_token: "c".into(),
            mfa_required: false,
            mfa_otp_id: String::new(),
            refresh_token_expires_in: 2,
            session_public_id: "p".into(),
        };
        let mapped = accept_invitation_response(signed_in);
        assert_eq!(mapped.user_id, "u");
        assert_eq!(mapped.session_id, "s");
        assert_eq!(mapped.access_token, "a");
        assert_eq!(mapped.refresh_token, "r");
        assert_eq!(mapped.access_token_expires_in, 1);
        assert_eq!(mapped.session_token, "st");
        assert_eq!(mapped.csrf_token, "c");
        assert_eq!(mapped.refresh_token_expires_in, 2);
        assert_eq!(mapped.session_public_id, "p");
    }

    #[tokio::test]
    async fn every_unusable_invitation_is_the_same_refusal() {
        // Malformed ids and empty tokens are refused before any store access,
        // with exactly the refusal an unknown, expired or revoked one gets.
        let svc = svc();
        let refusals = [
            svc.get_invitation_impl(Request::new(authn_pb::GetInvitationRequest {
                invitation_id: "not-a-uuid".into(),
                token: "inv_x".into(),
            }))
            .await
            .unwrap_err(),
            svc.start_invitation_acceptance_impl(Request::new(
                authn_pb::StartInvitationAcceptanceRequest {
                    invitation_id: Uuid::new_v4().to_string(),
                    token: "  ".into(),
                    ..Default::default()
                },
            ))
            .await
            .unwrap_err(),
            svc.accept_invitation_impl(Request::new(authn_pb::AcceptInvitationRequest {
                invitation_id: String::new(),
                token: "inv_x".into(),
                ..Default::default()
            }))
            .await
            .unwrap_err(),
        ];
        let expected = invitation_unavailable_status();
        for err in refusals {
            assert_eq!(err.code(), expected.code());
            assert_eq!(err.message(), expected.message());
            assert_eq!(
                decode_detail(&err).policy_decision_id,
                "invitation_unavailable"
            );
        }
    }

    #[tokio::test]
    async fn create_rejects_bad_input_before_touching_the_store() {
        let svc = svc();
        let admin = || test_claim_context("admin", "acme", "", &[MANAGE_INVITATIONS_SCOPE], &[]);
        let cases = [
            (
                authn_pb::CreateInvitationRequest {
                    channel: "fax".into(),
                    ..Default::default()
                },
                "channel",
            ),
            (
                authn_pb::CreateInvitationRequest {
                    channel: "SMS".into(),
                    address: "12345".into(),
                    ..Default::default()
                },
                "address",
            ),
            (
                authn_pb::CreateInvitationRequest {
                    channel: "PASSKEY".into(),
                    expires_in_seconds: 10,
                    ..Default::default()
                },
                "expires_in_seconds",
            ),
        ];
        for (request, field) in cases {
            let err = scope_claim_context_for_test(
                admin(),
                svc.create_invitation_impl(Request::new(request)),
            )
            .await
            .unwrap_err();
            assert_eq!(err.code(), tonic::Code::InvalidArgument, "{field}");
            assert_eq!(violated_field(&err), field);
        }
        // A reserved platform role is refused as a policy decision.
        let err = scope_claim_context_for_test(
            admin(),
            svc.create_invitation_impl(Request::new(authn_pb::CreateInvitationRequest {
                channel: "PASSKEY".into(),
                roles: strings(&["platform_admin"]),
                ..Default::default()
            })),
        )
        .await
        .unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    #[tokio::test]
    async fn management_is_confined_to_the_callers_tenant() {
        let svc = svc();
        // Another tenant in the body is refused for a tenant admin.
        let admin = test_claim_context("admin", "acme", "", &[MANAGE_INVITATIONS_SCOPE], &[]);
        let err = scope_claim_context_for_test(
            admin,
            svc.list_invitations_impl(Request::new(authn_pb::ListInvitationsRequest {
                tenant_id: "other-tenant".into(),
                ..Default::default()
            })),
        )
        .await
        .unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
        // Without the scope the call is refused before anything else.
        let reader = test_claim_context("reader", "acme", "", &["udb:read"], &[]);
        let err = scope_claim_context_for_test(
            reader,
            svc.list_invitations_impl(Request::new(authn_pb::ListInvitationsRequest::default())),
        )
        .await
        .unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
        // Foreign invitations look like missing ones.
        let foreign = test_claim_context("admin", "acme", "", &[MANAGE_INVITATIONS_SCOPE], &[]);
        let manages =
            scope_claim_context_for_test(foreign, async { caller_manages_tenant("other-tenant") })
                .await
                .expect("scope present");
        assert!(!manages);
    }
}
