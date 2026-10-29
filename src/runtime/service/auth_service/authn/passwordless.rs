//! 0.6.6 passwordless sign-in and the per-tenant sign-in policy.
//!
//! - `TenantAuthPolicy` (`udb_authn.tenant_auth_policies`, one row per tenant)
//!   says whether new accounts need a password, which channels may carry a
//!   sign-in code ("SMS", "EMAIL"), the https page a magic link opens and the
//!   tenant's step-up rules. A tenant with no row gets the defaults: passwords
//!   required, no passwordless channel. `PutTenantAuthPolicy` /
//!   `GetTenantAuthPolicy` are bearer RPCs bound to the caller's own tenant.
//! - `SendLoginOtp` (public) sends an `OTP_TYPE_LOGIN` code, or for EMAIL a
//!   magic link `<base>?otp_id=..&code=..`, to an account of the tenant. Its
//!   answer never reveals whether the address belongs to an account: an unknown
//!   address, a disabled channel, an account that may not sign in this way or
//!   a resend inside the cooldown all get a fresh otp_id that never verifies,
//!   and nothing is sent.
//! - `LoginWithOtp` (public) verifies the code (single use, attempt-limited,
//!   via `verify_otp_record`), refuses the accounts `Login` refuses, marks the
//!   proven phone/email verified, and issues exactly `Login`'s credentials
//!   through `complete_sign_in` (`auth_method` "otp", or "magic_link").
//! - `CreateUser` accepts an empty password when the tenant's policy has
//!   `password_required = false` ([`AuthnServiceImpl::tenant_password_required`]).

use super::login::SignIn;
use super::*;
use crate::runtime::native_catalog::native_model;
use sqlx::Row;

/// Manifest key `native_model` resolves the policy table from.
const TENANT_AUTH_POLICY_MSG: &str = "udb.core.authn.entity.v1.TenantAuthPolicy";

/// A tenant's sign-in policy was written.
const TOPIC_TENANT_AUTH_POLICY_CHANGED: &str = "udb.authn.tenant.auth_policy.changed.v1";

/// Channel names as stored in `passwordless_channels_json`.
const CHANNEL_SMS: &str = "SMS";
const CHANNEL_EMAIL: &str = "EMAIL";

/// `correlation_id` prefix of a sign-in code, by delivery form. LoginWithOtp
/// reads it back to stamp `auth_method` "otp" or "magic_link".
const LOGIN_CODE_CORRELATION_PREFIX: &str = "login_otp:code:";
const MAGIC_LINK_CORRELATION_PREFIX: &str = "login_otp:magic_link:";

/// Notification event a magic link is sent as (shipped en/bn templates render
/// its `link`).
const MAGIC_LINK_EVENT_TYPE: &str = "authn.magic_link";

/// Upper bounds on caller-supplied text.
const MAX_TENANT_ID_CHARS: usize = 120;
const MAX_MAGIC_LINK_BASE_URL_CHARS: usize = 2048;
const MAX_EMAIL_CHARS: usize = 255;

// ── Statuses ─────────────────────────────────────────────────────────────────

fn passwordless_invalid_fields<I, F, D>(message: impl Into<String>, fields: I) -> Status
where
    I: IntoIterator<Item = (F, D)>,
    F: Into<String>,
    D: Into<String>,
{
    crate::runtime::executor_utils::invalid_argument_fields(message, fields)
}

fn passwordless_internal_status(
    operation: impl Into<String>,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::internal_status("authn", operation, message)
}

fn otp_login_policy_status(policy_decision_id: &str, message: &str) -> Status {
    crate::runtime::executor_utils::policy_status_with_code(
        tonic::Code::PermissionDenied,
        "otp_login",
        policy_decision_id,
        message,
    )
}

/// The ONE refusal for a code that does not sign anyone in: unknown otp_id,
/// wrong/expired/used code, an OTP of another type, or a locked account.
fn invalid_login_code_status() -> Status {
    crate::runtime::executor_utils::unauthenticated_status(
        "invalid_login_code",
        "invalid or expired sign-in code",
    )
}

fn otp_login_user_not_active_status() -> Status {
    otp_login_policy_status("user_not_active", "user is not active")
}

fn otp_login_second_factor_required_status() -> Status {
    otp_login_policy_status(
        "second_factor_required",
        "a one-time code is a single factor and this account needs two: sign in with a passkey, or with the password and the second factor",
    )
}

fn otp_login_channel_disabled_status() -> Status {
    otp_login_policy_status(
        "passwordless_channel_disabled",
        "the tenant does not allow sign-in codes over this channel",
    )
}

fn auth_policy_revision_conflict_status() -> Status {
    crate::runtime::executor_utils::retryable_aborted_status(
        "authn",
        "put_tenant_auth_policy",
        0,
        "the tenant auth policy changed concurrently (expected_revision does not match); re-read it and retry",
    )
}

// ── Pure validation ──────────────────────────────────────────────────────────

/// A sign-in code channel named by a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LoginChannel {
    Sms,
    Email,
}

impl LoginChannel {
    /// The name stored in the tenant policy.
    fn policy_name(self) -> &'static str {
        match self {
            LoginChannel::Sms => CHANNEL_SMS,
            LoginChannel::Email => CHANNEL_EMAIL,
        }
    }

    /// The OTP record's `delivery_channel`.
    fn delivery_channel(self) -> &'static str {
        match self {
            LoginChannel::Sms => "sms",
            LoginChannel::Email => "email",
        }
    }

    fn from_delivery_channel(raw: &str) -> Option<Self> {
        if raw.eq_ignore_ascii_case("sms") {
            Some(LoginChannel::Sms)
        } else if raw.eq_ignore_ascii_case("email") {
            Some(LoginChannel::Email)
        } else {
            None
        }
    }
}

fn parse_login_channel(raw: &str) -> Result<LoginChannel, Status> {
    let raw = raw.trim();
    if raw.eq_ignore_ascii_case(CHANNEL_SMS) {
        Ok(LoginChannel::Sms)
    } else if raw.eq_ignore_ascii_case(CHANNEL_EMAIL) {
        Ok(LoginChannel::Email)
    } else {
        Err(passwordless_invalid_fields(
            "channel must be SMS or EMAIL",
            [("channel", "must be SMS or EMAIL")],
        ))
    }
}

/// The address a code goes to, normalized: an E.164 phone for SMS, a
/// lower-cased email for EMAIL.
fn normalized_login_address(channel: LoginChannel, raw: &str) -> Result<String, Status> {
    let address = raw.trim();
    match channel {
        LoginChannel::Sms if super::core::is_e164_phone(address) => Ok(address.to_string()),
        LoginChannel::Sms => Err(passwordless_invalid_fields(
            "address must be an E.164 phone number for SMS",
            [(
                "address",
                "must be an E.164 number: + then 7 to 15 digits, not starting with 0",
            )],
        )),
        LoginChannel::Email => {
            let valid = address.chars().count() <= MAX_EMAIL_CHARS
                && !address.chars().any(|c| c.is_whitespace() || c.is_control())
                && address
                    .split_once('@')
                    .is_some_and(|(local, domain)| !local.is_empty() && domain.contains('.'));
            if valid {
                Ok(address.to_ascii_lowercase())
            } else {
                Err(passwordless_invalid_fields(
                    "address must be an email address for EMAIL",
                    [(
                        "address",
                        "must be an email address of at most 255 characters",
                    )],
                ))
            }
        }
    }
}

/// The policy's channel list: each "SMS" or "EMAIL" (any case), upper-cased,
/// duplicates dropped, request order kept.
fn normalize_passwordless_channels(raw: &[String]) -> Result<Vec<String>, Status> {
    let mut channels: Vec<String> = Vec::new();
    for value in raw {
        let channel = parse_login_channel(value).map_err(|_| {
            passwordless_invalid_fields(
                format!("passwordless_channels contains an unknown channel {value:?}"),
                [("passwordless_channels", "each entry must be SMS or EMAIL")],
            )
        })?;
        let name = channel.policy_name().to_string();
        if !channels.contains(&name) {
            channels.push(name);
        }
    }
    Ok(channels)
}

/// Channels listed in a stored `passwordless_channels_json`; anything that is
/// not a JSON array of strings lists nothing (fail closed).
fn channels_from_json(raw: &str) -> Vec<String> {
    serde_json::from_str::<Vec<String>>(raw)
        .unwrap_or_default()
        .into_iter()
        .map(|channel| channel.trim().to_ascii_uppercase())
        .collect()
}

fn policy_allows_channel(
    policy: &authn_entity_pb::TenantAuthPolicy,
    channel: LoginChannel,
) -> bool {
    channels_from_json(&policy.passwordless_channels_json)
        .iter()
        .any(|listed| listed == channel.policy_name())
}

/// `magic_link_base_url`: empty, or an https URL with a host and no userinfo,
/// query string, fragment, whitespace or control characters.
fn validate_magic_link_base_url(raw: &str) -> Result<String, Status> {
    let url = raw.trim();
    if url.is_empty() {
        return Ok(String::new());
    }
    let host = url
        .strip_prefix("https://")
        .map(|rest| rest.split('/').next().unwrap_or_default())
        .unwrap_or_default();
    let valid = url.chars().count() <= MAX_MAGIC_LINK_BASE_URL_CHARS
        && !host.is_empty()
        && !host.contains('@')
        && !url
            .chars()
            .any(|c| c.is_whitespace() || c.is_control() || c == '?' || c == '#');
    if !valid {
        return Err(passwordless_invalid_fields(
            "magic_link_base_url must be an https URL without a query string or fragment",
            [(
                "magic_link_base_url",
                "must be empty or an https URL with a host, no user info, query string or fragment, at most 2048 characters",
            )],
        ));
    }
    Ok(url.to_string())
}

/// `step_up_rules_json`: empty means "[]"; otherwise a JSON array the step-up
/// module accepts. Stored compact.
fn normalize_step_up_rules_json(raw: &str) -> Result<String, Status> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Ok("[]".to_string());
    }
    let parsed: serde_json::Value = serde_json::from_str(raw).map_err(|_| {
        passwordless_invalid_fields(
            "step_up_rules_json must be a JSON array",
            [(
                "step_up_rules_json",
                "must be a JSON array of step-up rules",
            )],
        )
    })?;
    if !parsed.is_array() {
        return Err(passwordless_invalid_fields(
            "step_up_rules_json must be a JSON array",
            [(
                "step_up_rules_json",
                "must be a JSON array of step-up rules",
            )],
        ));
    }
    let compact = parsed.to_string();
    super::step_up::validate_step_up_rules(&compact)?;
    Ok(compact)
}

/// The link a magic-link email carries: `<base>?otp_id=<id>&code=<code>`.
fn magic_link_url(base: &str, otp_id: &str, code: &str) -> String {
    format!(
        "{}?otp_id={}&code={}",
        base.trim(),
        urlencoding::encode(otp_id),
        urlencoding::encode(code)
    )
}

/// The defaults of a tenant that never wrote a policy (revision 0).
fn default_tenant_auth_policy(tenant_id: &str) -> authn_entity_pb::TenantAuthPolicy {
    authn_entity_pb::TenantAuthPolicy {
        tenant_id: tenant_id.to_string(),
        password_required: true,
        passwordless_channels_json: "[]".to_string(),
        magic_link_base_url: String::new(),
        step_up_rules_json: "[]".to_string(),
        revision: 0,
        updated_by: String::new(),
        created_at: None,
        updated_at: None,
    }
}

/// The tenant a policy RPC acts on: required, bounded, and the caller's own
/// tenant unless the caller is a cross-tenant administrator (in-process calls
/// with no claim context are trusted, as for every authn admin RPC).
fn auth_policy_target_tenant(raw: &str) -> Result<String, Status> {
    let tenant_id = raw.trim();
    if tenant_id.is_empty() {
        return Err(passwordless_invalid_fields(
            "tenant_id is required",
            [("tenant_id", "must be a non-empty tenant id")],
        ));
    }
    if tenant_id.chars().count() > MAX_TENANT_ID_CHARS
        || tenant_id
            .chars()
            .any(|c| c.is_whitespace() || c.is_control())
    {
        return Err(passwordless_invalid_fields(
            "tenant_id is malformed",
            [(
                "tenant_id",
                "must be at most 120 characters with no whitespace",
            )],
        ));
    }
    let claim_ctx = crate::runtime::service::method_security::current_claim_context();
    crate::runtime::service::method_security::enforce_body_tenant_matches_claim(
        &claim_ctx, tenant_id, "",
    )?;
    Ok(tenant_id.to_string())
}

/// Whether `user` may receive and use a sign-in code at `now`: a person
/// account, ACTIVE or PENDING_VERIFICATION (as `Login` allows), not deleted,
/// not locked, without an enrolled second factor (a code alone is one factor),
/// and in `project_hint` when one was given.
fn otp_sign_in_eligible(user: &UserRecord, project_hint: &str, now: u64) -> bool {
    let project_hint = project_hint.trim();
    user.account_kind == authn::AccountKind::Person
        && matches!(
            user.status,
            authn::AccountStatus::Active | authn::AccountStatus::PendingVerification
        )
        && user.deleted_at_unix == 0
        && user.locked_until_unix <= now
        && !user.mfa_enabled
        && (project_hint.is_empty() || user.project_id.trim() == project_hint)
}

// ── Store ────────────────────────────────────────────────────────────────────

fn tenant_auth_policy_columns() -> Vec<&'static str> {
    vec![
        "tenant_id",
        "password_required",
        "passwordless_channels_json",
        "magic_link_base_url",
        "step_up_rules_json",
        "revision",
        "updated_by",
        "created_at",
        "updated_at",
    ]
}

fn tenant_auth_policy_select_clause() -> String {
    let m = native_model(TENANT_AUTH_POLICY_MSG, &tenant_auth_policy_columns());
    [
        m.text_or_empty_as("tenant_id", "tenant_id"),
        m.select_as("password_required", "password_required"),
        m.json_text_as("passwordless_channels_json", "passwordless_channels_json"),
        m.text_or_empty_as("magic_link_base_url", "magic_link_base_url"),
        m.json_text_as("step_up_rules_json", "step_up_rules_json"),
        format!("{} AS revision", m.q("revision")),
        m.text_or_empty_as("updated_by", "updated_by"),
        m.timestamp_unix_as("created_at", "created_at_unix"),
        m.timestamp_unix_as("updated_at", "updated_at_unix"),
    ]
    .join(", ")
}

/// A row that fails to decode degrades to the safe value: passwords required,
/// no channel, no link, no rule.
fn tenant_auth_policy_from_row(row: &sqlx::postgres::PgRow) -> authn_entity_pb::TenantAuthPolicy {
    authn_entity_pb::TenantAuthPolicy {
        tenant_id: row.try_get("tenant_id").unwrap_or_default(),
        password_required: row.try_get("password_required").unwrap_or(true),
        passwordless_channels_json: row
            .try_get("passwordless_channels_json")
            .unwrap_or_else(|_| "[]".to_string()),
        magic_link_base_url: row.try_get("magic_link_base_url").unwrap_or_default(),
        step_up_rules_json: row
            .try_get("step_up_rules_json")
            .unwrap_or_else(|_| "[]".to_string()),
        revision: row.try_get("revision").unwrap_or(0),
        updated_by: row.try_get("updated_by").unwrap_or_default(),
        created_at: timestamp_from_unix(
            row.try_get::<i64, _>("created_at_unix").unwrap_or(0).max(0) as u64,
        ),
        updated_at: timestamp_from_unix(
            row.try_get::<i64, _>("updated_at_unix").unwrap_or(0).max(0) as u64,
        ),
    }
}

async fn load_tenant_auth_policy_row<'c, E>(
    executor: E,
    tenant_id: &str,
    for_update: bool,
) -> Result<Option<authn_entity_pb::TenantAuthPolicy>, Status>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let m = native_model(TENANT_AUTH_POLICY_MSG, &tenant_auth_policy_columns());
    let sql = format!(
        "SELECT {cols} FROM {rel} WHERE {tenant} = $1{lock}",
        cols = tenant_auth_policy_select_clause(),
        rel = m.relation,
        tenant = m.q("tenant_id"),
        lock = if for_update { " FOR UPDATE" } else { "" },
    );
    let row = sqlx::query(&sql)
        .bind(tenant_id)
        .fetch_optional(executor)
        .await
        .map_err(|err| {
            passwordless_internal_status(
                "tenant_auth_policy_load",
                format!("tenant auth policy load failed: {err}"),
            )
        })?;
    Ok(row.map(|row| tenant_auth_policy_from_row(&row)))
}

/// Insert or replace the tenant's policy; a replace bumps `revision`.
async fn upsert_tenant_auth_policy(
    conn: &mut sqlx::PgConnection,
    policy: &authn_entity_pb::TenantAuthPolicy,
) -> Result<authn_entity_pb::TenantAuthPolicy, Status> {
    let m = native_model(TENANT_AUTH_POLICY_MSG, &tenant_auth_policy_columns());
    let sql = format!(
        "INSERT INTO {rel} AS p \
            ({tenant}, {password_required}, {channels}, {link}, {rules}, {revision}, {updated_by}) \
         VALUES ($1, $2, $3::JSONB, $4, $5::JSONB, 1, $6) \
         ON CONFLICT ({tenant}) DO UPDATE SET \
            {password_required} = EXCLUDED.{password_required}, \
            {channels} = EXCLUDED.{channels}, \
            {link} = EXCLUDED.{link}, \
            {rules} = EXCLUDED.{rules}, \
            {updated_by} = EXCLUDED.{updated_by}, \
            {revision} = p.{revision} + 1, \
            {updated_at} = now() \
         RETURNING {cols}",
        rel = m.relation,
        tenant = m.q("tenant_id"),
        password_required = m.q("password_required"),
        channels = m.q("passwordless_channels_json"),
        link = m.q("magic_link_base_url"),
        rules = m.q("step_up_rules_json"),
        revision = m.q("revision"),
        updated_by = m.q("updated_by"),
        updated_at = m.q("updated_at"),
        cols = tenant_auth_policy_select_clause(),
    );
    let row = sqlx::query(&sql)
        .bind(&policy.tenant_id)
        .bind(policy.password_required)
        .bind(&policy.passwordless_channels_json)
        .bind(&policy.magic_link_base_url)
        .bind(&policy.step_up_rules_json)
        .bind(&policy.updated_by)
        .fetch_one(&mut *conn)
        .await
        .map_err(|err| {
            passwordless_internal_status(
                "tenant_auth_policy_upsert",
                format!("tenant auth policy write failed: {err}"),
            )
        })?;
    Ok(tenant_auth_policy_from_row(&row))
}

fn build_tenant_auth_policy_event(
    policy: &authn_entity_pb::TenantAuthPolicy,
    actor: &str,
) -> AuthEvent {
    let step_up_rule_count =
        serde_json::from_str::<Vec<serde_json::Value>>(&policy.step_up_rules_json)
            .map(|rules| rules.len())
            .unwrap_or(0);
    AuthEvent::new(
        TOPIC_TENANT_AUTH_POLICY_CHANGED,
        policy.tenant_id.clone(),
        policy.tenant_id.clone(),
        serde_json::json!({
            "tenant_id": policy.tenant_id,
            "revision": policy.revision,
            "password_required": policy.password_required,
            "passwordless_channels": channels_from_json(&policy.passwordless_channels_json),
            "magic_link_enabled": !policy.magic_link_base_url.trim().is_empty(),
            "step_up_rule_count": step_up_rule_count,
        }),
    )
    .with_correlation(format!(
        "tenant_auth_policy:{}:{}",
        policy.tenant_id, policy.revision
    ))
    .with_compliance(ComplianceEnvelope {
        actor: actor.to_string(),
        target_resource: format!("tenant_auth_policy:{}", policy.tenant_id),
        target_tenant: policy.tenant_id.clone(),
        operation: "put_tenant_auth_policy".to_string(),
        outcome: "success".to_string(),
        reason_code: "tenant_auth_policy_written".to_string(),
        auth_method: super::super::grants::event_auth_method(),
        ..ComplianceEnvelope::default()
    })
}

impl AuthnServiceImpl {
    /// The tenant's sign-in policy, or the defaults (revision 0: passwords
    /// required, no passwordless channel, no magic link, no step-up rule) when
    /// the tenant never wrote one, the tenant id is empty or no Postgres store
    /// backs this service. For the other 0.6.6 areas: invitation links use
    /// `magic_link_base_url`, step-up reads `step_up_rules_json`.
    pub(super) async fn tenant_auth_policy(
        &self,
        tenant_id: &str,
    ) -> Result<authn_entity_pb::TenantAuthPolicy, Status> {
        let tenant_id = tenant_id.trim();
        let Some(pool) = self.pg_pool.as_ref() else {
            return Ok(default_tenant_auth_policy(tenant_id));
        };
        if tenant_id.is_empty() {
            return Ok(default_tenant_auth_policy(tenant_id));
        }
        Ok(load_tenant_auth_policy_row(pool, tenant_id, false)
            .await?
            .unwrap_or_else(|| default_tenant_auth_policy(tenant_id)))
    }

    /// Whether new accounts of the tenant need a password (CreateUser). True
    /// unless the tenant's policy says `password_required = false`.
    pub(super) async fn tenant_password_required(&self, tenant_id: &str) -> Result<bool, Status> {
        Ok(self.tenant_auth_policy(tenant_id).await?.password_required)
    }

    pub(super) async fn put_tenant_auth_policy_impl(
        &self,
        request: Request<authn_pb::PutTenantAuthPolicyRequest>,
    ) -> Result<Response<authn_pb::PutTenantAuthPolicyResponse>, Status> {
        let req = request.into_inner();
        let tenant_id = auth_policy_target_tenant(&req.tenant_id)?;
        let channels = normalize_passwordless_channels(&req.passwordless_channels)?;
        let magic_link_base_url = validate_magic_link_base_url(&req.magic_link_base_url)?;
        let step_up_rules_json = normalize_step_up_rules_json(&req.step_up_rules_json)?;
        let actor = super::super::grants::caller_updated_by();
        let desired = authn_entity_pb::TenantAuthPolicy {
            tenant_id: tenant_id.clone(),
            // Put replaces the whole policy; an unset flag keeps passwords
            // required (the safe default), never turns them off.
            password_required: req.password_required.unwrap_or(true),
            passwordless_channels_json: serde_json::to_string(&channels)
                .unwrap_or_else(|_| "[]".to_string()),
            magic_link_base_url,
            step_up_rules_json,
            updated_by: actor.clone(),
            ..default_tenant_auth_policy(&tenant_id)
        };
        // The revision check, the write and its audit event commit in ONE
        // transaction; the row lock serializes concurrent writers.
        let pool = self.require_pool()?;
        let mut tx = pool.begin().await.map_err(|err| {
            passwordless_internal_status(
                "put_tenant_auth_policy_tx_begin",
                format!("tenant auth policy tx begin failed: {err}"),
            )
        })?;
        let current = load_tenant_auth_policy_row(&mut *tx, &tenant_id, true).await?;
        let current_revision = current.as_ref().map_or(0, |policy| policy.revision);
        if req.expected_revision != 0 && req.expected_revision != current_revision {
            return Err(auth_policy_revision_conflict_status());
        }
        let stored = upsert_tenant_auth_policy(&mut *tx, &desired).await?;
        self.emit_event_in_tx(&mut *tx, build_tenant_auth_policy_event(&stored, &actor))
            .await?;
        tx.commit().await.map_err(|err| {
            passwordless_internal_status(
                "put_tenant_auth_policy_tx_commit",
                format!("tenant auth policy commit failed: {err}"),
            )
        })?;
        Ok(Response::new(authn_pb::PutTenantAuthPolicyResponse {
            policy: Some(stored),
        }))
    }

    pub(super) async fn get_tenant_auth_policy_impl(
        &self,
        request: Request<authn_pb::GetTenantAuthPolicyRequest>,
    ) -> Result<Response<authn_pb::GetTenantAuthPolicyResponse>, Status> {
        let req = request.into_inner();
        let tenant_id = auth_policy_target_tenant(&req.tenant_id)?;
        let policy = self.tenant_auth_policy(&tenant_id).await?;
        Ok(Response::new(authn_pb::GetTenantAuthPolicyResponse {
            is_default: policy.revision == 0,
            policy: Some(policy),
        }))
    }

    /// The tenant a public sign-in request names: a tenant UUID as is, else the
    /// tenant whose code it is, else the hint itself (deployments that key
    /// users by a literal tenant id). Never refuses: an unknown tenant simply
    /// has no accounts and the default policy.
    async fn resolve_sign_in_tenant(&self, hint: &str) -> String {
        let hint = hint.trim();
        if let Ok(tenant) = Uuid::parse_str(hint) {
            return tenant.to_string();
        }
        let Some(pool) = self.pg_pool.as_ref() else {
            return hint.to_string();
        };
        let m = native_model(
            "udb.core.tenant.entity.v1.Tenant",
            &["tenant_id", "code", "deleted_at"],
        );
        let sql = format!(
            "SELECT {tid}::text FROM {rel} WHERE {code} = $1 AND {del} IS NULL LIMIT 1",
            rel = m.relation,
            tid = m.q("tenant_id"),
            code = m.q("code"),
            del = m.q("deleted_at"),
        );
        match sqlx::query_scalar::<_, String>(&sql)
            .bind(hint)
            .fetch_optional(pool)
            .await
        {
            Ok(Some(tenant)) => tenant,
            Ok(None) => hint.to_string(),
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "sign-in tenant code lookup failed; using the tenant hint as given"
                );
                hint.to_string()
            }
        }
    }

    /// The one account of `tenant_id` that owns `address` on `channel`, if
    /// any. A phone shared by several accounts of the tenant names none of
    /// them (the code could not tell them apart).
    async fn sign_in_account_for_address(
        &self,
        tenant_id: &str,
        channel: LoginChannel,
        address: &str,
    ) -> Result<Option<UserRecord>, Status> {
        if tenant_id.is_empty() {
            return Ok(None);
        }
        match channel {
            LoginChannel::Email => self
                .users
                .get_user_by_email_in_tenant(address, tenant_id)
                .await
                .map_err(|err| {
                    passwordless_internal_status("send_login_otp_email_lookup", err.to_string())
                }),
            LoginChannel::Sms => {
                let pool = self.require_pool()?;
                let m = native_model(
                    "udb.core.authn.entity.v1.User",
                    &["user_id", "tenant_id", "phone", "deleted_at"],
                );
                let sql = format!(
                    "SELECT {uid}::text FROM {rel} \
                     WHERE {tenant} = $1 AND {phone} = $2 AND {del} IS NULL LIMIT 2",
                    uid = m.q("user_id"),
                    rel = m.relation,
                    tenant = m.q("tenant_id"),
                    phone = m.q("phone"),
                    del = m.q("deleted_at"),
                );
                let ids: Vec<String> = sqlx::query_scalar(&sql)
                    .bind(tenant_id)
                    .bind(address)
                    .fetch_all(pool)
                    .await
                    .map_err(|err| {
                        passwordless_internal_status(
                            "send_login_otp_phone_lookup",
                            format!("phone lookup failed: {err}"),
                        )
                    })?;
                let [user_id] = ids.as_slice() else {
                    return Ok(None);
                };
                self.users
                    .get_user_by_id_in_tenant(user_id, tenant_id)
                    .await
                    .map_err(|err| {
                        passwordless_internal_status("send_login_otp_user_load", err.to_string())
                    })
            }
        }
    }

    /// Persist a sign-in code sent as a magic link and deliver it. `None` when
    /// the per-user cooldown refused a new code (nothing written or sent).
    async fn issue_magic_link(
        &self,
        user: &UserRecord,
        address: &str,
        base_url: &str,
        now: u64,
    ) -> Result<Option<(String, String)>, Status> {
        let otp_type = authn_entity_pb::OtpType::Login as i32;
        let (record, code) = self.prepare_otp_record(
            user,
            otp_type,
            LoginChannel::Email.delivery_channel(),
            address,
            format!("{MAGIC_LINK_CORRELATION_PREFIX}{}", user.user_id),
            now,
        )?;
        let otp_id = record.otp_id.clone();
        if self
            .users
            .put_otp_with_cooldown(record, self.config.otp_cooldown_secs)
            .await
            .map_err(|err| passwordless_internal_status("magic_link_store", err.to_string()))?
            .is_some()
        {
            return Ok(None);
        }
        let link = magic_link_url(base_url, &otp_id, &code);
        self.deliver_magic_link(user, address, &code, &otp_id, &link)
            .await;
        Ok(Some((otp_id, code)))
    }

    /// Best-effort delivery of a magic link through the NotificationService
    /// template for `authn.magic_link` (the tenant's, else the shipped one, in
    /// the user's language; variables `link`, `code`, `expires_in_minutes`,
    /// `user_name`). A configured `UDB_OTP_DELIVERY_WEBHOOK_URL` wins, as for
    /// every code; its JSON body then carries `link` next to `code`.
    async fn deliver_magic_link(
        &self,
        user: &UserRecord,
        address: &str,
        code: &str,
        otp_id: &str,
        link: &str,
    ) {
        let otp_type = authn_entity_pb::OtpType::Login as i32;
        let channel = LoginChannel::Email.delivery_channel();
        let sent = !authn::otp_delivery_webhook_configured() && {
            let user_name = if user.full_name.trim().is_empty() {
                user.username.clone()
            } else {
                user.full_name.clone()
            };
            let variables = std::collections::HashMap::from([
                ("code".to_string(), code.to_string()),
                ("link".to_string(), link.to_string()),
                (
                    "expires_in_minutes".to_string(),
                    self.config.otp_ttl_secs.div_ceil(60).to_string(),
                ),
                ("user_name".to_string(), user_name),
            ]);
            // In the user's language, like every other sign-in code.
            let locale = self.user_notification_locale(user).await;
            self.send_localized_auth_notification(
                &user.tenant_id,
                &user.project_id,
                MAGIC_LINK_EVENT_TYPE,
                channel,
                &user.user_id,
                address,
                &locale,
                variables,
            )
            .await
        };
        if !sent {
            // The webhook body carries the link next to the code.
            authn::deliver_otp(channel, address, code, otp_type, &user.user_id, Some(link)).await;
        }
        #[cfg(test)]
        if let Ok(mut codes) = test_otp_codes().lock() {
            codes.insert(otp_id.to_string(), code.to_string());
        }
        #[cfg(not(test))]
        let _ = otp_id;
    }

    pub(super) async fn send_login_otp_impl(
        &self,
        request: Request<authn_pb::SendLoginOtpRequest>,
    ) -> Result<Response<authn_pb::SendLoginOtpResponse>, Status> {
        let req = request.into_inner();
        // Request-shape checks only: none of them depends on an account.
        if req.tenant_hint.trim().is_empty() {
            return Err(passwordless_invalid_fields(
                "tenant_hint is required",
                [("tenant_hint", "must name the tenant (its id or code)")],
            ));
        }
        let channel = parse_login_channel(&req.channel)?;
        let address = normalized_login_address(channel, &req.address)?;
        if req.magic_link && channel != LoginChannel::Email {
            return Err(passwordless_invalid_fields(
                "magic_link is only available for EMAIL",
                [("magic_link", "must be false unless channel is EMAIL")],
            ));
        }
        let now = now_unix();
        let expires_at_unix =
            i64::try_from(now.saturating_add(self.config.otp_ttl_secs)).unwrap_or(i64::MAX);
        // The uniform answer: a fresh id that never verifies, nothing sent, no
        // echo. Every "no" below returns exactly this, so the response does not
        // reveal whether the address belongs to an account.
        let uniform_miss = || {
            Response::new(authn_pb::SendLoginOtpResponse {
                otp_id: Uuid::new_v4().to_string(),
                expires_at_unix,
                dev_otp_code: String::new(),
            })
        };
        let tenant_id = self.resolve_sign_in_tenant(&req.tenant_hint).await;
        let policy = self.tenant_auth_policy(&tenant_id).await?;
        if !policy_allows_channel(&policy, channel) {
            return Ok(uniform_miss());
        }
        if req.magic_link && policy.magic_link_base_url.trim().is_empty() {
            tracing::warn!(
                tenant_id = %tenant_id,
                "SendLoginOtp: magic link requested but the tenant auth policy has no magic_link_base_url; nothing sent"
            );
            return Ok(uniform_miss());
        }
        let Some(user) = self
            .sign_in_account_for_address(&tenant_id, channel, &address)
            .await?
        else {
            return Ok(uniform_miss());
        };
        if !otp_sign_in_eligible(&user, &req.project_hint, now)
            || self
                .users
                .tenant_requires_mfa(&user.tenant_id)
                .await
                .map_err(|err| {
                    passwordless_internal_status(
                        "send_login_otp_tenant_mfa_policy",
                        err.to_string(),
                    )
                })?
        {
            return Ok(uniform_miss());
        }
        let otp_type = authn_entity_pb::OtpType::Login as i32;
        let issued = if req.magic_link {
            self.issue_magic_link(&user, &address, &policy.magic_link_base_url, now)
                .await?
        } else {
            match self
                .issue_otp_to(
                    &user,
                    otp_type,
                    channel.delivery_channel(),
                    &address,
                    format!("{LOGIN_CODE_CORRELATION_PREFIX}{}", user.user_id),
                    now,
                )
                .await
            {
                Ok(issued) => Some(issued),
                // A resend inside the cooldown issues nothing and answers like
                // an unknown address; the code already sent stays valid.
                Err(status) if status.code() == tonic::Code::ResourceExhausted => None,
                Err(status) => return Err(status),
            }
        };
        let Some((otp_id, code)) = issued else {
            return Ok(uniform_miss());
        };
        self.emit_event(
            AuthEvent::new(
                topics::OTP_SENT,
                otp_id.clone(),
                user.tenant_id.clone(),
                serde_json::json!({
                    "otp_id": otp_id.clone(),
                    "user_id": user.user_id.clone(),
                    "otp_type": otp_type,
                    "tenant_id": user.tenant_id.clone(),
                    "channel": channel.policy_name(),
                    "magic_link": req.magic_link,
                }),
            )
            .with_correlation(format!("otp_sent:{otp_id}"))
            .with_compliance(ComplianceEnvelope {
                actor: user.user_id.clone(),
                target_resource: user.user_id.clone(),
                operation: "login_otp_send".to_string(),
                outcome: "success".to_string(),
                reason_code: "login_otp_issued".to_string(),
                auth_method: "otp".to_string(),
                ..ComplianceEnvelope::default()
            }),
        )
        .await;
        // Dev-only echo under the single fail-closed gate, only for a code
        // that was really sent (the miss answer never carries one).
        let dev_otp_code = if super::mfa::otp_dev_echo_enabled() {
            code
        } else {
            String::new()
        };
        Ok(Response::new(authn_pb::SendLoginOtpResponse {
            otp_id,
            expires_at_unix,
            dev_otp_code,
        }))
    }

    /// Audit a refused sign-in code against its owner (when the otp_id names
    /// one; an unknown id has no subject to attribute to).
    async fn audit_failed_login_code(&self, otp_id: &str, ip_address: &str, user_agent: &str) {
        let Ok(Some(otp)) = self.users.get_otp(otp_id).await else {
            return;
        };
        self.emit_event(
            AuthEvent::new(
                topics::LOGIN_FAILED,
                otp.user_id.clone(),
                otp.tenant_id.clone(),
                serde_json::json!({
                    "user_id": otp.user_id.clone(),
                    "otp_id": otp_id,
                    "otp_type": otp.otp_type,
                    "ip_address": ip_address,
                    "stage": "login_code",
                }),
            )
            .with_correlation(format!("login_failed:{}", otp.user_id))
            .with_compliance(ComplianceEnvelope {
                actor: otp.user_id.clone(),
                target_resource: otp.user_id.clone(),
                operation: "login".to_string(),
                outcome: "failure".to_string(),
                reason_code: "invalid_login_code".to_string(),
                auth_method: "otp".to_string(),
                source_ip: ip_address.to_string(),
                user_agent: user_agent.to_string(),
                ..ComplianceEnvelope::default()
            }),
        )
        .await;
    }

    /// A successful sign-in code proves the channel it went to: an unverified
    /// email/phone that still matches the account becomes verified, and a
    /// PENDING_VERIFICATION account is activated the way VerifyOTP activates it
    /// (by its email, or by its phone when it has no email).
    async fn mark_login_channel_verified(
        &self,
        user: &mut UserRecord,
        channel: LoginChannel,
        address: &str,
        now: u64,
    ) -> Result<(), Status> {
        let pending = user.status == authn::AccountStatus::PendingVerification;
        match channel {
            LoginChannel::Email => {
                if !user.email.eq_ignore_ascii_case(address.trim()) {
                    return Ok(());
                }
                let newly_verified = user.email_verified_at_unix == 0;
                if newly_verified {
                    user.email_verified_at_unix = now;
                }
                if pending {
                    user.status = authn::AccountStatus::Active;
                }
                if newly_verified || pending {
                    user.updated_at_unix = now;
                    self.users.put_user(user.clone()).await.map_err(|err| {
                        passwordless_internal_status("otp_login_email_verify", err.to_string())
                    })?;
                }
                if newly_verified {
                    self.emit_event(
                        AuthEvent::new(
                            topics::EMAIL_VERIFIED,
                            user.user_id.clone(),
                            user.tenant_id.clone(),
                            serde_json::json!({
                                "user_id": user.user_id.clone(),
                                "email": user.email.clone(),
                            }),
                        )
                        .with_correlation(format!("email_verified:{}", user.user_id))
                        .with_compliance(ComplianceEnvelope {
                            actor: user.user_id.clone(),
                            target_resource: user.user_id.clone(),
                            operation: "email_verify".to_string(),
                            outcome: "success".to_string(),
                            reason_code: "login_otp_verified".to_string(),
                            auth_method: "otp".to_string(),
                            ..ComplianceEnvelope::default()
                        }),
                    )
                    .await;
                }
            }
            LoginChannel::Sms => {
                if user.phone.trim() != address.trim() {
                    return Ok(());
                }
                let newly_verified = user.phone_verified_at_unix == 0;
                if newly_verified {
                    self.users
                        .mark_phone_verified(&user.user_id, now)
                        .await
                        .map_err(|err| {
                            passwordless_internal_status("otp_login_phone_verify", err.to_string())
                        })?;
                    user.phone_verified_at_unix = now;
                }
                if pending && user.email.trim().is_empty() {
                    user.status = authn::AccountStatus::Active;
                    user.updated_at_unix = now;
                    self.users.put_user(user.clone()).await.map_err(|err| {
                        passwordless_internal_status("otp_login_phone_activate", err.to_string())
                    })?;
                }
                if newly_verified {
                    self.emit_event(
                        AuthEvent::new(
                            topics::PHONE_VERIFIED,
                            user.user_id.clone(),
                            user.tenant_id.clone(),
                            serde_json::json!({ "user_id": user.user_id.clone() }),
                        )
                        .with_correlation(format!("phone_verified:{}", user.user_id))
                        .with_compliance(ComplianceEnvelope {
                            actor: user.user_id.clone(),
                            target_resource: user.user_id.clone(),
                            operation: "phone_verify".to_string(),
                            outcome: "success".to_string(),
                            reason_code: "login_otp_verified".to_string(),
                            auth_method: "otp".to_string(),
                            ..ComplianceEnvelope::default()
                        }),
                    )
                    .await;
                }
            }
        }
        Ok(())
    }

    pub(super) async fn login_with_otp_impl(
        &self,
        request: Request<authn_pb::LoginWithOtpRequest>,
    ) -> Result<Response<authn_pb::LoginWithOtpResponse>, Status> {
        let req = request.into_inner();
        Self::require_uuid_arg(&req.otp_id, "otp_id")?;
        if req.code.trim().is_empty() {
            return Err(passwordless_invalid_fields(
                "code is required",
                [("code", "must be the delivered sign-in code")],
            ));
        }
        let now = now_unix();
        // Single use and attempt-limited: a wrong guess counts against the
        // code, a few of them expire it, and only one concurrent submission of
        // the right code wins. Only an OTP_TYPE_LOGIN code signs anyone in.
        let Some(record) = self
            .verify_otp_record(
                &req.otp_id,
                &req.code,
                Some(authn_entity_pb::OtpType::Login as i32),
                now,
            )
            .await?
        else {
            self.metrics.record_auth_login(false);
            self.audit_failed_login_code(&req.otp_id, &req.ip_address, &req.user_agent)
                .await;
            return Err(invalid_login_code_status());
        };
        let Some(mut user) = self
            .users
            .get_user_by_id(&record.user_id)
            .await
            .map_err(|err| passwordless_internal_status("otp_login_user_load", err.to_string()))?
            .filter(|user| user.deleted_at_unix == 0)
        else {
            self.metrics.record_auth_login(false);
            return Err(invalid_login_code_status());
        };
        // A locked account answers like a bad code (as Login answers a locked
        // account like a bad password).
        if user.locked_until_unix > now {
            self.metrics.record_auth_login(false);
            return Err(invalid_login_code_status());
        }
        // The code is proven: account state may now be named.
        if user.account_kind != authn::AccountKind::Person
            || !matches!(
                user.status,
                authn::AccountStatus::Active | authn::AccountStatus::PendingVerification
            )
        {
            self.metrics.record_auth_login(false);
            return Err(otp_login_user_not_active_status());
        }
        if user.mfa_enabled
            || self
                .users
                .tenant_requires_mfa(&user.tenant_id)
                .await
                .map_err(|err| {
                    passwordless_internal_status("otp_login_tenant_mfa_policy", err.to_string())
                })?
        {
            self.metrics.record_auth_login(false);
            return Err(otp_login_second_factor_required_status());
        }
        // The tenant may have turned the channel off since the code was sent.
        let channel = LoginChannel::from_delivery_channel(&record.delivery_channel)
            .ok_or_else(otp_login_channel_disabled_status)?;
        if !policy_allows_channel(&self.tenant_auth_policy(&user.tenant_id).await?, channel) {
            self.metrics.record_auth_login(false);
            return Err(otp_login_channel_disabled_status());
        }
        self.mark_login_channel_verified(&mut user, channel, &record.delivery_address, now)
            .await?;
        user.failed_login_count = 0;
        user.last_login_at_unix = now;
        user.updated_at_unix = now;
        self.users.put_user(user.clone()).await.map_err(|err| {
            passwordless_internal_status("otp_login_success_update", err.to_string())
        })?;
        let magic_link = record
            .correlation_id
            .starts_with(MAGIC_LINK_CORRELATION_PREFIX);
        let auth_method = if magic_link { "magic_link" } else { "otp" };
        let signed_in = self
            .complete_sign_in(
                user,
                SignIn {
                    device_name: &req.device_name,
                    ip_address: &req.ip_address,
                    user_agent: &req.user_agent,
                    device_id: &req.device_id,
                    // Person accounts take their scopes from their role
                    // grants; requested scopes narrow service accounts only.
                    requested_scopes: &[],
                    token_auth_method: auth_method,
                    audit_auth_method: auth_method,
                    audit_reason_code: "login_code_verified",
                    restore_on_failure: None,
                },
                now,
            )
            .await?;
        Ok(Response::new(login_with_otp_response(signed_in)))
    }
}

/// LoginWithOtpResponse carries LoginResponse's fields under the same names
/// and numbers.
fn login_with_otp_response(signed_in: authn_pb::LoginResponse) -> authn_pb::LoginWithOtpResponse {
    authn_pb::LoginWithOtpResponse {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::udb::core::authn::services::v1::authn_service_server::AuthnService;
    use crate::proto::{ErrorDetail, ErrorKind};
    use crate::runtime::authn::AuthnConfig;
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;
    use crate::runtime::security::SecurityConfig;
    use tonic::{Code, Request};

    fn svc() -> AuthnServiceImpl {
        AuthnServiceImpl::new(AuthnConfig::default(), SecurityConfig::default())
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

    fn assert_field_violation(status: &Status, field: &str) {
        assert_eq!(status.code(), Code::InvalidArgument);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert!(
            detail.field_violations.iter().any(|v| v.field == field),
            "expected a field violation on {field}, got {:?}",
            detail.field_violations
        );
    }

    fn person(status: authn::AccountStatus) -> UserRecord {
        UserRecord {
            user_id: Uuid::new_v4().to_string(),
            username: "otp_person".to_string(),
            email: "otp.person@example.com".to_string(),
            account_kind: authn::AccountKind::Person,
            status,
            tenant_id: "tenant-a".to_string(),
            project_id: "billing".to_string(),
            phone: "+8801712345678".to_string(),
            ..UserRecord::default()
        }
    }

    #[test]
    fn channels_parse_case_insensitively_and_dedupe() {
        let channels = normalize_passwordless_channels(&[
            "sms".to_string(),
            " Email ".to_string(),
            "SMS".to_string(),
        ])
        .expect("known channels");
        assert_eq!(channels, vec!["SMS".to_string(), "EMAIL".to_string()]);
        let err = normalize_passwordless_channels(&["push".to_string()])
            .expect_err("unknown channel is refused");
        assert_field_violation(&err, "passwordless_channels");
    }

    #[test]
    fn stored_channels_fail_closed_on_malformed_json() {
        let mut policy = default_tenant_auth_policy("tenant-a");
        assert!(!policy_allows_channel(&policy, LoginChannel::Sms));
        policy.passwordless_channels_json = r#"["sms","EMAIL"]"#.to_string();
        assert!(policy_allows_channel(&policy, LoginChannel::Sms));
        assert!(policy_allows_channel(&policy, LoginChannel::Email));
        policy.passwordless_channels_json = r#"{"SMS":true}"#.to_string();
        assert!(!policy_allows_channel(&policy, LoginChannel::Sms));
    }

    #[test]
    fn default_policy_requires_passwords_and_enables_nothing() {
        let policy = default_tenant_auth_policy("tenant-a");
        assert!(policy.password_required);
        assert_eq!(policy.revision, 0);
        assert_eq!(policy.passwordless_channels_json, "[]");
        assert_eq!(policy.step_up_rules_json, "[]");
        assert!(policy.magic_link_base_url.is_empty());
    }

    #[test]
    fn magic_link_base_url_must_be_plain_https() {
        assert_eq!(validate_magic_link_base_url("  ").unwrap(), "");
        assert_eq!(
            validate_magic_link_base_url(" https://app.example.com/auth/link ").unwrap(),
            "https://app.example.com/auth/link"
        );
        for bad in [
            "http://app.example.com/link",
            "https://",
            "https:///path",
            "https://user@app.example.com/link",
            "https://app.example.com/link?next=/",
            "https://app.example.com/link#frag",
            "https://app.example.com/a link",
            "javascript:alert(1)",
        ] {
            let err = validate_magic_link_base_url(bad).expect_err(bad);
            assert_field_violation(&err, "magic_link_base_url");
        }
        let long = format!("https://app.example.com/{}", "a".repeat(2048));
        assert_field_violation(
            &validate_magic_link_base_url(&long).expect_err("too long"),
            "magic_link_base_url",
        );
    }

    #[test]
    fn magic_link_carries_the_otp_id_and_code() {
        assert_eq!(
            magic_link_url("https://app.example.com/link", "8d6c", "123456"),
            "https://app.example.com/link?otp_id=8d6c&code=123456"
        );
    }

    #[test]
    fn step_up_rules_must_be_a_json_array() {
        assert_eq!(normalize_step_up_rules_json("").unwrap(), "[]");
        for bad in ["{", r#"{"action":"x"}"#, "42"] {
            let err = normalize_step_up_rules_json(bad).expect_err(bad);
            assert_field_violation(&err, "step_up_rules_json");
        }
    }

    #[test]
    fn login_addresses_are_validated_per_channel() {
        assert_eq!(
            normalized_login_address(LoginChannel::Sms, " +8801712345678 ").unwrap(),
            "+8801712345678"
        );
        assert_field_violation(
            &normalized_login_address(LoginChannel::Sms, "01712345678").unwrap_err(),
            "address",
        );
        assert_eq!(
            normalized_login_address(LoginChannel::Email, " Ana@Example.COM ").unwrap(),
            "ana@example.com"
        );
        for bad in ["ana", "@example.com", "ana@localhost", "a na@example.com"] {
            assert_field_violation(
                &normalized_login_address(LoginChannel::Email, bad).unwrap_err(),
                "address",
            );
        }
        assert_field_violation(&parse_login_channel("push").unwrap_err(), "channel");
        assert_eq!(parse_login_channel(" sms ").unwrap(), LoginChannel::Sms);
    }

    #[test]
    fn only_active_unlocked_single_factor_people_may_sign_in_by_code() {
        let now = 1_000_000;
        assert!(otp_sign_in_eligible(
            &person(authn::AccountStatus::Active),
            "",
            now
        ));
        assert!(otp_sign_in_eligible(
            &person(authn::AccountStatus::PendingVerification),
            "billing",
            now
        ));
        for status in [
            authn::AccountStatus::Suspended,
            authn::AccountStatus::Locked,
            authn::AccountStatus::Deactivated,
            authn::AccountStatus::PasswordSetupRequired,
        ] {
            assert!(!otp_sign_in_eligible(&person(status), "", now));
        }
        let mut locked = person(authn::AccountStatus::Active);
        locked.locked_until_unix = now + 60;
        assert!(!otp_sign_in_eligible(&locked, "", now));
        let mut mfa = person(authn::AccountStatus::Active);
        mfa.mfa_enabled = true;
        assert!(!otp_sign_in_eligible(&mfa, "", now));
        let mut service = person(authn::AccountStatus::Active);
        service.account_kind = authn::AccountKind::ServiceAccount;
        assert!(!otp_sign_in_eligible(&service, "", now));
        let mut deleted = person(authn::AccountStatus::Active);
        deleted.deleted_at_unix = now - 1;
        assert!(!otp_sign_in_eligible(&deleted, "", now));
        assert!(!otp_sign_in_eligible(
            &person(authn::AccountStatus::Active),
            "other-project",
            now
        ));
    }

    #[test]
    fn delivery_channel_round_trips() {
        for channel in [LoginChannel::Sms, LoginChannel::Email] {
            assert_eq!(
                LoginChannel::from_delivery_channel(channel.delivery_channel()),
                Some(channel)
            );
        }
        assert_eq!(LoginChannel::from_delivery_channel("push"), None);
    }

    #[test]
    fn login_response_fields_map_one_to_one() {
        let mapped = login_with_otp_response(authn_pb::LoginResponse {
            user_id: "u".into(),
            session_id: "s".into(),
            access_token: "a".into(),
            refresh_token: "r".into(),
            access_token_expires_in: 900,
            session_token: "st".into(),
            csrf_token: "c".into(),
            mfa_required: false,
            mfa_otp_id: String::new(),
            refresh_token_expires_in: 86_400,
            session_public_id: "sesspub_x".into(),
        });
        assert_eq!(mapped.user_id, "u");
        assert_eq!(mapped.session_id, "s");
        assert_eq!(mapped.access_token, "a");
        assert_eq!(mapped.refresh_token, "r");
        assert_eq!(mapped.access_token_expires_in, 900);
        assert_eq!(mapped.session_token, "st");
        assert_eq!(mapped.csrf_token, "c");
        assert!(!mapped.mfa_required);
        assert_eq!(mapped.refresh_token_expires_in, 86_400);
        assert_eq!(mapped.session_public_id, "sesspub_x");
    }

    #[test]
    fn invalid_login_code_is_one_opaque_refusal() {
        let status = invalid_login_code_status();
        assert_eq!(status.code(), Code::Unauthenticated);
        assert_eq!(status.message(), "invalid or expired sign-in code");
        assert_eq!(
            decode_detail(&status).policy_decision_id,
            "invalid_login_code"
        );
    }

    #[tokio::test]
    async fn send_login_otp_rejects_malformed_requests_before_any_lookup() {
        let service = svc();
        let missing_tenant = service
            .send_login_otp(Request::new(authn_pb::SendLoginOtpRequest {
                channel: "SMS".into(),
                address: "+8801712345678".into(),
                ..Default::default()
            }))
            .await
            .expect_err("tenant_hint is required");
        assert_field_violation(&missing_tenant, "tenant_hint");
        let sms_link = service
            .send_login_otp(Request::new(authn_pb::SendLoginOtpRequest {
                tenant_hint: "acme".into(),
                channel: "SMS".into(),
                address: "+8801712345678".into(),
                magic_link: true,
                ..Default::default()
            }))
            .await
            .expect_err("magic links are EMAIL only");
        assert_field_violation(&sms_link, "magic_link");
    }

    #[tokio::test]
    async fn send_login_otp_without_policy_store_answers_uniformly_and_sends_nothing() {
        // No Postgres store: the tenant has the default policy (no channel),
        // so the answer is the uniform miss.
        let response = svc()
            .send_login_otp(Request::new(authn_pb::SendLoginOtpRequest {
                tenant_hint: "acme".into(),
                channel: "EMAIL".into(),
                address: "someone@example.com".into(),
                ..Default::default()
            }))
            .await
            .expect("uniform answer")
            .into_inner();
        assert!(Uuid::parse_str(&response.otp_id).is_ok());
        assert!(response.dev_otp_code.is_empty());
        assert!(response.expires_at_unix > 0);
        let codes = test_otp_codes().lock().expect("codes");
        assert!(!codes.contains_key(&response.otp_id));
    }

    #[tokio::test]
    async fn login_with_otp_validates_its_arguments() {
        let service = svc();
        let bad_id = service
            .login_with_otp(Request::new(authn_pb::LoginWithOtpRequest {
                otp_id: "not-a-uuid".into(),
                code: "123456".into(),
                ..Default::default()
            }))
            .await
            .expect_err("otp_id must be a UUID");
        assert_eq!(bad_id.code(), Code::InvalidArgument);
        let no_code = service
            .login_with_otp(Request::new(authn_pb::LoginWithOtpRequest {
                otp_id: Uuid::new_v4().to_string(),
                ..Default::default()
            }))
            .await
            .expect_err("code is required");
        assert_field_violation(&no_code, "code");
    }

    #[tokio::test]
    async fn auth_policy_rpcs_require_a_tenant() {
        let service = svc();
        let put = service
            .put_tenant_auth_policy(Request::new(authn_pb::PutTenantAuthPolicyRequest::default()))
            .await
            .expect_err("tenant_id is required");
        assert_field_violation(&put, "tenant_id");
        let get = service
            .get_tenant_auth_policy(Request::new(authn_pb::GetTenantAuthPolicyRequest::default()))
            .await
            .expect_err("tenant_id is required");
        assert_field_violation(&get, "tenant_id");
    }

    #[tokio::test]
    async fn auth_policy_rpcs_refuse_another_tenant_for_a_tenant_bound_caller() {
        use crate::runtime::service::method_security::{
            scope_claim_context_for_test, test_claim_context,
        };
        let service = svc();
        let ctx = test_claim_context(
            "admin-a",
            "tenant-a",
            "",
            &["udb:authn:manage-auth-policy", "udb:authn:read-auth-policy"],
            &[],
        );
        let put = scope_claim_context_for_test(
            ctx.clone(),
            service.put_tenant_auth_policy(Request::new(authn_pb::PutTenantAuthPolicyRequest {
                tenant_id: "tenant-b".into(),
                password_required: Some(false),
                ..Default::default()
            })),
        )
        .await
        .expect_err("cross-tenant put is refused");
        assert_eq!(put.code(), Code::PermissionDenied);
        let get = scope_claim_context_for_test(
            ctx.clone(),
            service.get_tenant_auth_policy(Request::new(authn_pb::GetTenantAuthPolicyRequest {
                tenant_id: "tenant-b".into(),
                ..Default::default()
            })),
        )
        .await
        .expect_err("cross-tenant get is refused");
        assert_eq!(get.code(), Code::PermissionDenied);
        // Its own tenant reads the defaults (no store here).
        let own = scope_claim_context_for_test(
            ctx,
            service.get_tenant_auth_policy(Request::new(authn_pb::GetTenantAuthPolicyRequest {
                tenant_id: "tenant-a".into(),
                ..Default::default()
            })),
        )
        .await
        .expect("own tenant")
        .into_inner();
        assert!(own.is_default);
        assert!(own.policy.expect("policy").password_required);
    }
}
