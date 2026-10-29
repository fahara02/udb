//! 0.6.6 step-up authentication and the assurance claims of sign-in tokens.
//!
//! * **Tenant step-up rules** — `TenantAuthPolicy.step_up_rules_json` (the
//!   PASSWORDLESS area owns the row) is a JSON list of
//!   `{action, max_age_seconds, factors}`. [`validate_step_up_rules`] is the one
//!   validator (PutTenantAuthPolicy calls it); [`StepUpRule`] is its parsed form.
//! * **Step-up challenge** — IssueMfaChallenge with an `action` sends the
//!   EMAIL/SMS code (OTP type SENSITIVE_OPERATION) or starts the WebAuthn
//!   assertion; VerifyMfaChallenge checks the proof (`lifecycle.rs`
//!   `verify_mfa_proof`) and, for the challenged user, mints a short-lived
//!   step-up JWT (`acr = aal2`, `amr`, `auth_time`, `step_up_action`) that is
//!   NOT an access token.
//! * **CheckStepUp** — a resource server asks whether a step-up token
//!   satisfies the tenant rule for an action right now.
//! * **Assurance claims** — the `pwd_*` sign-in methods a password login with
//!   a second factor is stamped with (`acr = aal2`, `amr = ["pwd", "otp"]`);
//!   the claim mapping itself lives in `runtime::security`.
//! * **Localized auth messages** — the recipient's locale for the shipped
//!   English/Bangla `authn.*` templates, and the send helper the invitation
//!   and passwordless flows use.

use super::*;

/// Longest accepted step-up action name.
const MAX_STEP_UP_ACTION_CHARS: usize = 120;
/// Bounds of a rule's `max_age_seconds`.
const MIN_STEP_UP_MAX_AGE_SECS: i64 = 30;
const MAX_STEP_UP_MAX_AGE_SECS: i64 = 3600;
/// Max age (and step-up token lifetime) for an action the tenant has no rule for.
pub(super) const DEFAULT_STEP_UP_MAX_AGE_SECS: i64 = 300;
/// Most rules one tenant policy may hold.
const MAX_STEP_UP_RULES: usize = 200;
/// The factor names a rule may list. `OTP` means any one-time code sent or
/// generated for the user: `EMAIL_OTP`, `SMS_OTP` or `TOTP` (not a recovery
/// code, which is a fallback a tenant must allow explicitly).
const STEP_UP_RULE_FACTORS: &[&str] = &[
    "OTP",
    "SMS_OTP",
    "EMAIL_OTP",
    "TOTP",
    "WEBAUTHN",
    "RECOVERY_CODE",
];

const AUTH_FACTOR_KIND_PREFIX: &str = "AUTH_FACTOR_KIND_";

fn step_up_invalid_fields<I, F, D>(message: impl Into<String>, fields: I) -> Status
where
    I: IntoIterator<Item = (F, D)>,
    F: Into<String>,
    D: Into<String>,
{
    crate::runtime::executor_utils::invalid_argument_fields(message, fields)
}

fn step_up_internal_status(operation: impl Into<String>, message: impl Into<String>) -> Status {
    crate::runtime::executor_utils::internal_status("authn", operation, message)
}

/// One tenant step-up rule: performing `action` needs a second factor from
/// `factors`, proven at most `max_age_seconds` ago.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct StepUpRule {
    pub(super) action: String,
    pub(super) max_age_seconds: i64,
    pub(super) factors: Vec<String>,
}

impl StepUpRule {
    /// Whether a step-up proven with `factor` (an `AuthFactorKind` short name
    /// such as `EMAIL_OTP`) satisfies this rule.
    pub(super) fn allows_factor(&self, factor: &str) -> bool {
        let factor = factor.trim().to_ascii_uppercase();
        self.factors.iter().any(|allowed| {
            let allowed = allowed.trim().to_ascii_uppercase();
            allowed == factor
                || (allowed == "OTP" && matches!(factor.as_str(), "EMAIL_OTP" | "SMS_OTP" | "TOTP"))
        })
    }
}

/// Validate a tenant's `step_up_rules_json`: a JSON array (at most 200) of
/// `{action, max_age_seconds, factors}` objects, no other keys. `action` is
/// non-empty, at most 120 characters and unique within the list;
/// `max_age_seconds` is 30..=3600; `factors` is a non-empty subset of
/// `OTP | SMS_OTP | EMAIL_OTP | TOTP | WEBAUTHN | RECOVERY_CODE`. Every
/// violation is reported as a field violation under `step_up_rules_json`.
pub(super) fn validate_step_up_rules(json: &str) -> Result<(), Status> {
    parse_step_up_rules(json).map(|_| ())
}

/// [`validate_step_up_rules`] returning the parsed rules (empty input = none).
pub(super) fn parse_step_up_rules(json: &str) -> Result<Vec<StepUpRule>, Status> {
    let json = json.trim();
    if json.is_empty() {
        return Ok(Vec::new());
    }
    let rules: Vec<StepUpRule> = serde_json::from_str(json).map_err(|err| {
        step_up_invalid_fields(
            "step_up_rules_json must be a JSON array of {action, max_age_seconds, factors}",
            [(
                "step_up_rules_json".to_string(),
                format!("not a list of step-up rules: {err}"),
            )],
        )
    })?;
    if rules.len() > MAX_STEP_UP_RULES {
        return Err(step_up_invalid_fields(
            "too many step-up rules",
            [(
                "step_up_rules_json".to_string(),
                format!("at most {MAX_STEP_UP_RULES} rules"),
            )],
        ));
    }
    let mut violations: Vec<(String, String)> = Vec::new();
    let mut seen_actions: Vec<String> = Vec::with_capacity(rules.len());
    for (index, rule) in rules.iter().enumerate() {
        let field = |name: &str| format!("step_up_rules_json[{index}].{name}");
        let action = rule.action.trim();
        if action.is_empty() {
            violations.push((field("action"), "must not be empty".to_string()));
        } else if action.chars().count() > MAX_STEP_UP_ACTION_CHARS {
            violations.push((
                field("action"),
                format!("must be at most {MAX_STEP_UP_ACTION_CHARS} characters"),
            ));
        } else if action.chars().any(char::is_control) {
            violations.push((
                field("action"),
                "must not contain control characters".to_string(),
            ));
        } else if seen_actions.iter().any(|seen| seen == action) {
            violations.push((
                field("action"),
                format!("duplicate rule for action '{action}'"),
            ));
        } else {
            seen_actions.push(action.to_string());
        }
        if !(MIN_STEP_UP_MAX_AGE_SECS..=MAX_STEP_UP_MAX_AGE_SECS).contains(&rule.max_age_seconds) {
            violations.push((
                field("max_age_seconds"),
                format!(
                    "must be between {MIN_STEP_UP_MAX_AGE_SECS} and {MAX_STEP_UP_MAX_AGE_SECS}"
                ),
            ));
        }
        if rule.factors.is_empty() {
            violations.push((
                field("factors"),
                "must list at least one factor".to_string(),
            ));
        }
        for factor in &rule.factors {
            let normalized = factor.trim().to_ascii_uppercase();
            if !STEP_UP_RULE_FACTORS.contains(&normalized.as_str()) {
                violations.push((
                    field("factors"),
                    format!(
                        "unknown factor '{factor}'; use {}",
                        STEP_UP_RULE_FACTORS.join(", ")
                    ),
                ));
            }
        }
    }
    if !violations.is_empty() {
        return Err(step_up_invalid_fields(
            "step_up_rules_json has invalid rules",
            violations,
        ));
    }
    Ok(rules
        .into_iter()
        .map(|rule| StepUpRule {
            action: rule.action.trim().to_string(),
            max_age_seconds: rule.max_age_seconds,
            factors: rule
                .factors
                .iter()
                .map(|factor| factor.trim().to_ascii_uppercase())
                .collect(),
        })
        .collect())
}

/// The trimmed action of a step-up request (`IssueMfaChallenge.action`,
/// `CheckStepUp.action`); empty stays empty (no step-up).
pub(super) fn normalize_step_up_action(raw: &str) -> Result<String, Status> {
    let action = raw.trim();
    if action.chars().count() > MAX_STEP_UP_ACTION_CHARS {
        return Err(step_up_invalid_fields(
            format!("action must be at most {MAX_STEP_UP_ACTION_CHARS} characters"),
            [(
                "action",
                format!("must be at most {MAX_STEP_UP_ACTION_CHARS} characters"),
            )],
        ));
    }
    if action.chars().any(char::is_control) {
        return Err(step_up_invalid_fields(
            "action must not contain control characters",
            [("action", "must not contain control characters")],
        ));
    }
    Ok(action.to_string())
}

/// `AUTH_FACTOR_KIND_EMAIL_OTP` (the stored enum name) → `EMAIL_OTP`.
pub(super) fn factor_short_name(factor_db: &str) -> String {
    let upper = factor_db.trim().to_ascii_uppercase();
    upper
        .strip_prefix(AUTH_FACTOR_KIND_PREFIX)
        .unwrap_or(&upper)
        .to_string()
}

/// The `auth_method` a password login is stamped with (drives `acr`/`amr`):
/// `pwd` without MFA; with MFA, the second factor login.rs verified, checked
/// in the same order (recovery code, then email/SMS code, then authenticator
/// app), and `pwd_webauthn` when none of those was sent (the passkey
/// assertion is the only other accepted second factor).
pub(super) fn password_sign_in_auth_method(
    mfa_enabled: bool,
    recovery_code: &str,
    mfa_otp_id: &str,
    totp_code: &str,
) -> &'static str {
    if !mfa_enabled {
        "pwd"
    } else if !recovery_code.trim().is_empty() {
        "pwd_recovery"
    } else if !mfa_otp_id.trim().is_empty() {
        "pwd_otp"
    } else if !totp_code.trim().is_empty() {
        "pwd_totp"
    } else {
        "pwd_webauthn"
    }
}

/// Notification event (template) an OTP of `otp_type` is delivered with.
/// Every one-time code uses a code template (`code`, `expires_in_minutes`,
/// `user_name`): an invitation-acceptance code renders through `authn.otp`
/// because the `authn.invitation` template is the invitation message itself
/// (`link`, `tenant_name`, `inviter_name`, `expires_in_days`), sent with
/// [`AuthnServiceImpl::send_localized_auth_notification`].
pub(super) fn otp_notification_event_type(otp_type: i32) -> &'static str {
    use authn_entity_pb::OtpType;
    match OtpType::try_from(otp_type).unwrap_or(OtpType::Unspecified) {
        OtpType::PasswordReset => "authn.password_reset",
        OtpType::EmailVerification => "authn.email_verification",
        OtpType::Login
        | OtpType::Invitation
        | OtpType::Login2fa
        | OtpType::SensitiveOperation
        | OtpType::PhoneVerification
        | OtpType::Unspecified => "authn.otp",
    }
}

/// Mask a delivery address for display: `rahim@example.com` →
/// `r***@example.com`, `+8801712345621` → `+88017******21` (first six and last
/// two characters of a phone number kept).
pub(super) fn mask_delivery_address(address: &str) -> String {
    let address = address.trim();
    if let Some((local, domain)) = address.split_once('@') {
        let first: String = local.chars().take(1).collect();
        return format!("{first}***@{domain}");
    }
    let chars: Vec<char> = address.chars().collect();
    if chars.len() <= 6 {
        return "*".repeat(chars.len());
    }
    let head: String = chars[..6].iter().collect();
    let tail: String = chars[chars.len() - 2..].iter().collect();
    format!("{head}{}{tail}", "*".repeat(chars.len() - 8))
}

/// What a consumed MFA challenge row binds the proof to.
#[derive(Debug, Clone, Default)]
pub(super) struct ChallengeBinding {
    /// The step-up action (empty = plain second-factor check).
    pub(super) action: String,
    /// EMAIL_OTP / SMS_OTP: the OTP issued with the challenge.
    pub(super) otp_id: String,
    /// WEBAUTHN: the assertion challenge started with the challenge.
    pub(super) webauthn_challenge_id: String,
}

/// Factor material IssueMfaChallenge prepared before storing the challenge.
#[derive(Debug, Clone, Default)]
pub(super) struct PreparedMfaFactor {
    pub(super) otp_id: String,
    pub(super) otp_destination_masked: String,
    pub(super) webauthn_challenge_id: String,
    pub(super) webauthn_options_json: String,
}

impl AuthnServiceImpl {
    /// The tenant's parsed step-up rules. Rules were validated when stored;
    /// a stored list that no longer parses fails closed.
    pub(super) async fn tenant_step_up_rules(
        &self,
        tenant_id: &str,
    ) -> Result<Vec<StepUpRule>, Status> {
        let policy = self.tenant_auth_policy(tenant_id).await?;
        parse_step_up_rules(&policy.step_up_rules_json).map_err(|_| {
            step_up_internal_status(
                "step_up_rules_load",
                "the tenant's stored step-up rules are not valid",
            )
        })
    }

    /// The tenant's rule for `action`, if it has one.
    pub(super) async fn tenant_step_up_rule(
        &self,
        tenant_id: &str,
        action: &str,
    ) -> Result<Option<StepUpRule>, Status> {
        Ok(self
            .tenant_step_up_rules(tenant_id)
            .await?
            .into_iter()
            .find(|rule| rule.action == action))
    }

    /// IssueMfaChallenge: refuse up front a factor the tenant's rule for
    /// `action` does not accept (no code is sent for a challenge that could
    /// never satisfy the rule). No action or no rule: any second factor.
    pub(super) async fn ensure_step_up_factor_allowed(
        &self,
        tenant_id: &str,
        action: &str,
        factor: i32,
    ) -> Result<(), Status> {
        if action.is_empty() {
            return Ok(());
        }
        let factor_name = factor_short_name(
            authn_entity_pb::AuthFactorKind::try_from(factor)
                .unwrap_or(authn_entity_pb::AuthFactorKind::Unspecified)
                .as_str_name(),
        );
        match self.tenant_step_up_rule(tenant_id, action).await? {
            Some(rule) if !rule.allows_factor(&factor_name) => {
                Err(crate::runtime::executor_utils::policy_status(
                    "issue_mfa_challenge",
                    "step_up_factor_not_allowed",
                    format!(
                        "the tenant's step-up rule for '{action}' does not accept {factor_name}; use one of {}",
                        rule.factors.join(", ")
                    ),
                ))
            }
            _ => Ok(()),
        }
    }

    /// IssueMfaChallenge: send the code (EMAIL_OTP / SMS_OTP, OTP type
    /// SENSITIVE_OPERATION, to the user's VERIFIED address only) or start the
    /// WebAuthn assertion (WEBAUTHN) this challenge is answered with. TOTP and
    /// recovery codes need nothing up front; PASSWORD is not a second factor.
    pub(super) async fn prepare_mfa_challenge_factor(
        &self,
        user: &UserRecord,
        factor: i32,
        challenge_id: &str,
        now: u64,
    ) -> Result<PreparedMfaFactor, Status> {
        use authn_entity_pb::AuthFactorKind;
        let correlation_id = format!("mfa_challenge:{challenge_id}");
        match AuthFactorKind::try_from(factor).unwrap_or(AuthFactorKind::Unspecified) {
            AuthFactorKind::EmailOtp => {
                if user.email.trim().is_empty() || user.email_verified_at_unix == 0 {
                    return Err(crate::runtime::executor_utils::policy_status(
                        "issue_mfa_challenge",
                        "email_not_verified",
                        "an EMAIL_OTP challenge needs a verified email address",
                    ));
                }
                let (otp_id, _code) = self
                    .issue_otp_to(
                        user,
                        authn_entity_pb::OtpType::SensitiveOperation as i32,
                        "email",
                        &user.email,
                        correlation_id,
                        now,
                    )
                    .await?;
                Ok(PreparedMfaFactor {
                    otp_id,
                    otp_destination_masked: mask_delivery_address(&user.email),
                    ..PreparedMfaFactor::default()
                })
            }
            AuthFactorKind::SmsOtp => {
                if user.phone.trim().is_empty() || user.phone_verified_at_unix == 0 {
                    return Err(crate::runtime::executor_utils::policy_status(
                        "issue_mfa_challenge",
                        "phone_not_verified",
                        "an SMS_OTP challenge needs a verified phone number",
                    ));
                }
                let (otp_id, _code) = self
                    .issue_otp_to(
                        user,
                        authn_entity_pb::OtpType::SensitiveOperation as i32,
                        "sms",
                        &user.phone,
                        correlation_id,
                        now,
                    )
                    .await?;
                Ok(PreparedMfaFactor {
                    otp_id,
                    otp_destination_masked: mask_delivery_address(&user.phone),
                    ..PreparedMfaFactor::default()
                })
            }
            AuthFactorKind::Webauthn => {
                // PASSKEYS' assertion start (refused without the `webauthn`
                // build or when the user holds no passkey).
                let (webauthn_challenge_id, webauthn_options_json) = self
                    .start_webauthn_assertion_for_user(&user.user_id, &user.tenant_id)
                    .await?;
                Ok(PreparedMfaFactor {
                    webauthn_challenge_id,
                    webauthn_options_json,
                    ..PreparedMfaFactor::default()
                })
            }
            AuthFactorKind::Password => Err(step_up_invalid_fields(
                "factor_kind must be a second factor",
                [(
                    "factor_kind",
                    "PASSWORD is not a second factor; use TOTP, EMAIL_OTP, SMS_OTP, WEBAUTHN or RECOVERY_CODE",
                )],
            )),
            AuthFactorKind::Totp | AuthFactorKind::RecoveryCode | AuthFactorKind::Unspecified => {
                Ok(PreparedMfaFactor::default())
            }
        }
    }

    /// VerifyMfaChallenge's answer. A verified challenge that named an action
    /// also gets a step-up token, but only when the caller IS the challenged
    /// user (an administrator verifying for someone else gets `verified`
    /// alone) and a JWT signing key is configured. The token reuses the
    /// caller's session-bound token id, so revoking that session revokes it.
    pub(super) async fn verify_mfa_challenge_response(
        &self,
        verified: bool,
        user_id: String,
        factor_db: &str,
        binding: &ChallengeBinding,
        now: u64,
    ) -> Result<authn_pb::VerifyMfaChallengeResponse, Status> {
        if !verified {
            return Ok(authn_pb::VerifyMfaChallengeResponse::default());
        }
        let mut response = authn_pb::VerifyMfaChallengeResponse {
            verified: true,
            user_id: user_id.clone(),
            ..Default::default()
        };
        if binding.action.is_empty() {
            return Ok(response);
        }
        let caller = crate::runtime::service::method_security::claim_context_present()
            .then(crate::runtime::service::method_security::current_claim_context);
        if let Some(caller) = &caller
            && caller.authenticated
            && caller.subject.trim() != user_id
        {
            return Ok(response);
        }
        let Some(user) = self
            .users
            .get_user_by_id(&user_id)
            .await
            .map_err(|err| step_up_internal_status("step_up_user_load", err.to_string()))?
        else {
            return Ok(response);
        };
        let max_age = self
            .tenant_step_up_rule(&user.tenant_id, &binding.action)
            .await?
            .map(|rule| rule.max_age_seconds)
            .unwrap_or(DEFAULT_STEP_UP_MAX_AGE_SECS);
        let jti = caller
            .as_ref()
            .map(|caller| caller.credential_id.trim().to_string())
            .filter(|id| id.starts_with("sess_") || id.starts_with("rtf_"))
            .unwrap_or_else(|| format!("stepup_{}", Uuid::new_v4().simple()));
        let factor = factor_short_name(factor_db);
        match crate::runtime::security::sign_step_up_token(
            &self.security,
            crate::runtime::security::StepUpGrant {
                subject: &user.user_id,
                tenant_id: &user.tenant_id,
                project_id: &user.project_id,
                jti: &jti,
                action: &binding.action,
                factor: &factor,
            },
            now,
            max_age.max(1) as u64,
        ) {
            Ok(Some((token, exp))) => {
                response.step_up_token = token;
                response.step_up_expires_at_unix = exp;
                response.acr = "aal2".to_string();
                response.amr = crate::runtime::security::amr_for_step_up_factor(&factor)
                    .map(|amr| vec![amr.to_string()])
                    .unwrap_or_default();
            }
            Ok(None) => {}
            Err(err) => {
                tracing::warn!(error = %err, "step-up token signing failed; returning verified without a token");
            }
        }
        Ok(response)
    }

    /// CheckStepUp: does `step_up_token` satisfy the tenant's step-up rule
    /// for `action` right now? Checks, in order: the token (signature, expiry,
    /// `token_use`, live user and session), the caller (a person must be the
    /// token's user; a service caller must share its tenant unless it is a
    /// cross-tenant admin), the action, the factor against the rule, and the
    /// age of the proof against the rule's `max_age_seconds` (300 without a
    /// rule). A failed check is an answer (`satisfied = false` + reason), not
    /// an error.
    pub(super) async fn check_step_up_impl(
        &self,
        request: Request<authn_pb::CheckStepUpRequest>,
    ) -> Result<Response<authn_pb::CheckStepUpResponse>, Status> {
        let req = request.into_inner();
        let action = normalize_step_up_action(&req.action)?;
        if action.is_empty() {
            return Err(step_up_invalid_fields(
                "action is required",
                [("action", "must name the protected action")],
            ));
        }
        if req.step_up_token.trim().is_empty() {
            return Err(step_up_invalid_fields(
                "step_up_token is required",
                [("step_up_token", "must be the token from VerifyMfaChallenge")],
            ));
        }
        let denied = |reason: &str| authn_pb::CheckStepUpResponse {
            satisfied: false,
            reason: reason.to_string(),
            ..Default::default()
        };
        let now = now_unix();
        let Ok((claims, assurance)) = crate::runtime::security::validate_step_up_token(
            &self.security,
            req.step_up_token.trim(),
        ) else {
            return Ok(Response::new(denied("invalid_token")));
        };
        if matches!(
            self.jwt_persisted_state_user(&claims, now).await?,
            JwtPersistedState::Rejected
        ) {
            return Ok(Response::new(denied("invalid_token")));
        }
        let subject = claims.sub.clone().unwrap_or_default();
        let tenant_id = claims.tenant_id.clone().unwrap_or_default();
        let factor = assurance.step_up_factor.clone();
        let mut response = authn_pb::CheckStepUpResponse {
            satisfied: false,
            reason: String::new(),
            acr: assurance.acr.clone(),
            amr: assurance.amr.clone(),
            auth_time: assurance.auth_time,
            user_id: subject.clone(),
            factor_kind: authn_entity_pb::AuthFactorKind::from_str_name(&format!(
                "{AUTH_FACTOR_KIND_PREFIX}{factor}"
            ))
            .map(|kind| kind as i32)
            .unwrap_or_default(),
            max_age_seconds: DEFAULT_STEP_UP_MAX_AGE_SECS,
        };
        if crate::runtime::service::method_security::claim_context_present() {
            let caller = crate::runtime::service::method_security::current_claim_context();
            let same_caller = if caller.service_identity.trim().is_empty() {
                caller.subject.trim() == subject
            } else {
                caller.is_cross_tenant_admin() || caller.tenant_id.trim() == tenant_id
            };
            if !same_caller {
                response.reason = "subject_mismatch".to_string();
                return Ok(Response::new(response));
            }
        }
        if assurance.step_up_action != action {
            response.reason = "action_mismatch".to_string();
            return Ok(Response::new(response));
        }
        let rule = self.tenant_step_up_rule(&tenant_id, &action).await?;
        if let Some(rule) = &rule {
            response.max_age_seconds = rule.max_age_seconds;
            if !rule.allows_factor(&factor) {
                response.reason = "factor_not_allowed".to_string();
                return Ok(Response::new(response));
            }
        }
        let age = (now as i64).saturating_sub(assurance.auth_time);
        if assurance.auth_time <= 0 || age > response.max_age_seconds {
            response.reason = "too_old".to_string();
            return Ok(Response::new(response));
        }
        response.satisfied = true;
        response.reason = "satisfied".to_string();
        Ok(Response::new(response))
    }

    /// The locale the user's auth messages render in: the `users.locale`
    /// column (read with the user), else a `locale` profile attribute, else
    /// empty (English).
    pub(super) async fn user_notification_locale(&self, user: &UserRecord) -> String {
        if !user.locale.trim().is_empty() {
            return user.locale.trim().to_string();
        }
        serde_json::from_str::<serde_json::Value>(&user.profile_attributes_json)
            .ok()
            .and_then(|attributes| {
                attributes
                    .get("locale")
                    .and_then(serde_json::Value::as_str)
                    .map(|locale| locale.trim().to_string())
            })
            .unwrap_or_default()
    }

    /// Send one of the broker's own `authn.*` messages (for example the
    /// `authn.invitation` message or an `authn.magic_link`) through the mounted
    /// NotificationService in `locale` (empty = English), using the shipped
    /// English/Bangla template when the tenant has none. `channel` is `"sms"`
    /// or `"email"`. Returns whether it was queued; `false` (logged) when no
    /// notifier is mounted, no template applies, or the send failed, so the
    /// caller can fall back to another delivery.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn send_localized_auth_notification(
        &self,
        tenant_id: &str,
        project_id: &str,
        event_type: &str,
        channel: &str,
        recipient_id: &str,
        address: &str,
        locale: &str,
        variables: std::collections::HashMap<String, String>,
    ) -> bool {
        let Some(notifier) = &self.system_notifier else {
            return false;
        };
        use crate::proto::udb::core::notification::entity::v1::NotificationChannel;
        let notification_channel = if channel.eq_ignore_ascii_case("sms") {
            NotificationChannel::Sms
        } else {
            NotificationChannel::Email
        } as i32;
        match notifier
            .send_system_localized(
                tenant_id,
                project_id,
                event_type,
                notification_channel,
                recipient_id,
                address,
                locale,
                variables,
            )
            .await
        {
            Ok(sent) => sent,
            Err(err) => {
                tracing::warn!(event_type, error = %err, "auth notification failed");
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn field_names(status: &Status) -> Vec<String> {
        let raw = status
            .metadata()
            .get_bin(crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY)
            .expect("typed detail trailer is present")
            .to_bytes()
            .expect("typed detail trailer decodes to bytes");
        let detail = crate::runtime::executor_utils::decode_error_detail_from_raw(&raw);
        detail
            .field_violations
            .into_iter()
            .map(|violation| violation.field)
            .collect()
    }

    #[test]
    fn valid_rules_parse_and_normalize() {
        assert_eq!(parse_step_up_rules("").unwrap(), Vec::new());
        assert_eq!(parse_step_up_rules("[]").unwrap(), Vec::new());
        let rules = parse_step_up_rules(
            r#"[{"action":" payments.approve ","max_age_seconds":120,"factors":["otp","WEBAUTHN"]},
                {"action":"users.delete","max_age_seconds":3600,"factors":["RECOVERY_CODE"]}]"#,
        )
        .unwrap();
        assert_eq!(rules.len(), 2);
        assert_eq!(rules[0].action, "payments.approve");
        assert_eq!(rules[0].factors, ["OTP", "WEBAUTHN"]);
        assert!(
            validate_step_up_rules(
                r#"[{"action":"a","max_age_seconds":30,"factors":["SMS_OTP","EMAIL_OTP","TOTP"]}]"#
            )
            .is_ok()
        );
    }

    #[test]
    fn invalid_rules_name_every_violation() {
        let too_long = "x".repeat(121);
        let status = validate_step_up_rules(&format!(
            r#"[{{"action":"","max_age_seconds":29,"factors":[]}},
                {{"action":"{too_long}","max_age_seconds":3601,"factors":["PASSWORD"]}},
                {{"action":"dup","max_age_seconds":60,"factors":["TOTP"]}},
                {{"action":"dup","max_age_seconds":60,"factors":["TOTP"]}}]"#
        ))
        .unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        let fields = field_names(&status);
        for expected in [
            "step_up_rules_json[0].action",
            "step_up_rules_json[0].max_age_seconds",
            "step_up_rules_json[0].factors",
            "step_up_rules_json[1].action",
            "step_up_rules_json[1].max_age_seconds",
            "step_up_rules_json[1].factors",
            "step_up_rules_json[3].action",
        ] {
            assert!(
                fields.iter().any(|f| f == expected),
                "{expected} in {fields:?}"
            );
        }
        for malformed in [
            "{}",
            "not json",
            r#"[{"action":"a","max_age_seconds":60}]"#,
            r#"[{"action":"a","max_age_seconds":60,"factors":["TOTP"],"extra":1}]"#,
            r#"[{"action":"a","max_age_seconds":"60","factors":["TOTP"]}]"#,
        ] {
            let status = validate_step_up_rules(malformed).unwrap_err();
            assert_eq!(status.code(), tonic::Code::InvalidArgument, "{malformed}");
            assert_eq!(field_names(&status), ["step_up_rules_json"], "{malformed}");
        }
    }

    #[test]
    fn rule_factor_matching_treats_otp_as_any_one_time_code() {
        let rule = StepUpRule {
            action: "a".into(),
            max_age_seconds: 60,
            factors: vec!["OTP".into()],
        };
        for allowed in ["EMAIL_OTP", "SMS_OTP", "TOTP", "totp"] {
            assert!(rule.allows_factor(allowed), "{allowed}");
        }
        for refused in ["WEBAUTHN", "RECOVERY_CODE", "PASSWORD", ""] {
            assert!(!rule.allows_factor(refused), "{refused}");
        }
        let webauthn_only = StepUpRule {
            factors: vec!["WEBAUTHN".into()],
            ..rule
        };
        assert!(webauthn_only.allows_factor("WEBAUTHN"));
        assert!(!webauthn_only.allows_factor("EMAIL_OTP"));
    }

    #[test]
    fn step_up_action_is_bounded() {
        assert_eq!(normalize_step_up_action("  pay  ").unwrap(), "pay");
        assert_eq!(normalize_step_up_action("").unwrap(), "");
        assert!(normalize_step_up_action(&"a".repeat(121)).is_err());
        assert!(normalize_step_up_action("a\nb").is_err());
    }

    /// The password login's sign-in method follows the second factor
    /// login.rs accepted, so the token's `amr` names it and `acr` is aal2.
    #[test]
    fn password_login_maps_second_factor_to_amr() {
        use crate::runtime::security::{acr_for_auth_method, amr_for_auth_method};
        let cases = [
            ((false, "", "", ""), "pwd", "aal1", vec!["pwd"]),
            (
                (true, "rc-1", "otp-1", "123456"),
                "pwd_recovery",
                "aal2",
                vec!["pwd", "otp"],
            ),
            (
                (true, "", "otp-1", "123456"),
                "pwd_otp",
                "aal2",
                vec!["pwd", "otp"],
            ),
            (
                (true, "", "", "123456"),
                "pwd_totp",
                "aal2",
                vec!["pwd", "otp"],
            ),
            (
                (true, "", "", ""),
                "pwd_webauthn",
                "aal2",
                vec!["pwd", "webauthn"],
            ),
        ];
        for ((mfa, recovery, otp_id, totp), method, acr, amr) in cases {
            let got = password_sign_in_auth_method(mfa, recovery, otp_id, totp);
            assert_eq!(got, method);
            assert_eq!(acr_for_auth_method(got), acr, "{method}");
            assert_eq!(
                amr_for_auth_method(got).map(<[&str]>::to_vec),
                Some(amr),
                "{method}"
            );
        }
    }

    #[test]
    fn otp_types_map_to_code_templates() {
        use authn_entity_pb::OtpType;
        assert_eq!(
            otp_notification_event_type(OtpType::PasswordReset as i32),
            "authn.password_reset"
        );
        assert_eq!(
            otp_notification_event_type(OtpType::EmailVerification as i32),
            "authn.email_verification"
        );
        for code in [
            OtpType::Login,
            OtpType::Invitation,
            OtpType::Login2fa,
            OtpType::SensitiveOperation,
            OtpType::PhoneVerification,
        ] {
            assert_eq!(
                otp_notification_event_type(code as i32),
                "authn.otp",
                "{code:?}"
            );
        }
    }

    #[test]
    fn factor_names_and_addresses() {
        assert_eq!(factor_short_name("AUTH_FACTOR_KIND_EMAIL_OTP"), "EMAIL_OTP");
        assert_eq!(factor_short_name("TOTP"), "TOTP");
        assert_eq!(
            mask_delivery_address("rahim@example.com"),
            "r***@example.com"
        );
        assert_eq!(mask_delivery_address("+8801712345621"), "+88017******21");
        assert_eq!(mask_delivery_address("12345"), "*****");
    }
}
