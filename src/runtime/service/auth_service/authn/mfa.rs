//! Multi-factor auth: email/SMS OTP issuance + verification, TOTP enrollment,
//! recovery codes, the per-tenant MFA-enforcement policy, and phone verification.

use super::*;

/// Pure decision for the dev-echo gate: echo is permitted ONLY when the
/// `UDB_OTP_DEV_ECHO` env opt-in is set AND the broker is NOT in a production
/// posture. Factored out (and `pub(super)`) so the production-closed property is
/// deterministically unit-testable without touching the process-wide env/OnceLock.
/// Mirrors `webauthn_softauth::test_mode_enabled`'s fail-closed posture.
pub(crate) fn otp_dev_echo_resolved(env_opt_in: bool, is_production: bool) -> bool {
    env_opt_in && !is_production
}

/// Whether the broker should echo plaintext OTP codes in `SendOTP`/`ForgotPassword`/
/// `SendPhoneVerification` responses. Resolved once from `UDB_OTP_DEV_ECHO` (a
/// dev/conformance affordance — must NEVER export proof material in production).
/// Fail-closed in production posture regardless of the env var. This is the SINGLE
/// chokepoint every echo site reads. bug_report.md F/Lane-2.
pub(crate) fn otp_dev_echo_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        let env_opt_in = std::env::var("UDB_OTP_DEV_ECHO")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        otp_dev_echo_resolved(
            env_opt_in,
            crate::runtime::security::SecurityConfig::current().is_production(),
        )
    })
}

fn mfa_required_field(
    field: &'static str,
    description: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::invalid_argument_fields(message, [(field, description)])
}

fn mfa_internal_status(operation: impl Into<String>, message: impl Into<String>) -> Status {
    crate::runtime::executor_utils::internal_status("authn", operation, message)
}

fn mfa_webauthn_enrollment_rpc_required_status() -> Status {
    crate::runtime::executor_utils::policy_status(
        "mfa_enrollment",
        "webauthn_enrollment_rpc_required",
        "WebAuthn enrollment uses StartWebAuthnRegistration and FinishWebAuthnRegistration",
    )
}

impl AuthnServiceImpl {
    pub(super) async fn issue_otp(
        &self,
        user: &UserRecord,
        otp_type: i32,
        correlation_id: String,
        now: u64,
    ) -> Result<(String, String), Status> {
        let address = user.email.clone();
        self.issue_otp_to(user, otp_type, "email", &address, correlation_id, now)
            .await
    }

    /// Issue an OTP delivered over an explicit channel/address (email or sms).
    pub(super) async fn issue_otp_to(
        &self,
        user: &UserRecord,
        otp_type: i32,
        channel: &str,
        address: &str,
        correlation_id: String,
        now: u64,
    ) -> Result<(String, String), Status> {
        let (rec, code) =
            self.prepare_otp_record(user, otp_type, channel, address, correlation_id, now);
        let otp_id = rec.otp_id.clone();
        self.users
            .put_otp(rec)
            .await
            .map_err(|err| mfa_internal_status("issue_otp_store", err.to_string()))?;
        // Best-effort outbound delivery to the operator's channel gateway. A
        // delivery failure (or no configured webhook) never fails issuance — the
        // OTP is persisted and the caller holds the otp_id.
        self.deliver_otp_code(channel, address, &code, otp_type, &user.user_id, &otp_id)
            .await;
        Ok((otp_id, code))
    }

    /// Build a fresh PENDING OTP record (and its plaintext code) WITHOUT touching
    /// the store or sending a notification. Splitting issuance into prepare →
    /// persist → deliver lets a caller persist the row INSIDE its own transaction
    /// (so the OTP commits atomically with its sibling mutation, §7) and run the
    /// best-effort, non-rollback-able delivery only AFTER the tx commits.
    pub(super) fn prepare_otp_record(
        &self,
        user: &UserRecord,
        otp_type: i32,
        channel: &str,
        address: &str,
        correlation_id: String,
        now: u64,
    ) -> (OtpRecord, String) {
        let otp_id = Uuid::new_v4().to_string();
        let code = authn::mfa_challenge::generate_otp_code();
        let rec = OtpRecord {
            otp_id,
            user_id: user.user_id.clone(),
            otp_type,
            code_hash: authn::hash_otp_code(&code, &self.otp_hash_key()),
            delivery_channel: channel.to_string(),
            delivery_address: address.to_string(),
            status: authn_entity_pb::OtpStatus::Pending as i32,
            attempt_count: 0,
            superseded_by_id: String::new(),
            expires_at_unix: now.saturating_add(self.config.otp_ttl_secs),
            used_at_unix: 0,
            created_at_unix: now,
            correlation_id,
            // Denormalize the owning user's tenant so the OTP row is tenant-scoped
            // (RLS tolerates NULL, so an empty tenant simply stores NULL).
            tenant_id: user.tenant_id.clone(),
        };
        (rec, code)
    }

    /// Best-effort post-persistence OTP delivery (operator channel gateway + the
    /// test-only code capture). Never fails the caller: the OTP is already durable
    /// and the caller holds the otp_id. Safe to call AFTER a committing tx so the
    /// notification side-effect is post-commit (it cannot be rolled back).
    pub(super) async fn deliver_otp_code(
        &self,
        channel: &str,
        address: &str,
        code: &str,
        otp_type: i32,
        user_id: &str,
        otp_id: &str,
    ) {
        authn::deliver_otp(channel, address, code, otp_type, user_id).await;
        #[cfg(test)]
        if let Ok(mut codes) = test_otp_codes().lock() {
            codes.insert(otp_id.to_string(), code.to_string());
        }
        #[cfg(not(test))]
        let _ = otp_id;
    }

    /// Enforce the configured per-(user, OTP type) cooldown to throttle OTP
    /// flooding. No-op when `otp_cooldown_secs` is 0 or no prior OTP of that type
    /// exists for the user.
    pub(super) async fn enforce_otp_cooldown(
        &self,
        user_id: &str,
        otp_type: i32,
        now: u64,
    ) -> Result<(), Status> {
        let cooldown = self.config.otp_cooldown_secs;
        if cooldown == 0 {
            return Ok(());
        }
        if let Some(last) = self
            .users
            .latest_otp_created_at(user_id, otp_type)
            .await
            .map_err(|err| mfa_internal_status("otp_cooldown_lookup", err.to_string()))?
        {
            let ready_at = last.saturating_add(cooldown);
            if now < ready_at {
                let retry_after_secs = ready_at - now;
                return Err(crate::runtime::executor_utils::quota_status(
                    "authn",
                    "otp cooldown",
                    retry_after_secs as i64 * 1_000,
                    format!("OTP cooldown active; retry in {retry_after_secs} seconds"),
                ));
            }
        }
        Ok(())
    }

    pub(super) async fn verify_otp_record(
        &self,
        otp_id: &str,
        code: &str,
        expected_type: Option<i32>,
        now: u64,
    ) -> Result<Option<OtpRecord>, Status> {
        let Some(mut rec) = self
            .users
            .get_otp(otp_id)
            .await
            .map_err(|err| mfa_internal_status("verify_otp_load", err.to_string()))?
        else {
            return Ok(None);
        };
        if expected_type.is_some_and(|kind| rec.otp_type != kind) {
            return Ok(None);
        }
        if rec.status != authn_entity_pb::OtpStatus::Pending as i32 || now >= rec.expires_at_unix {
            rec.status = authn_entity_pb::OtpStatus::Expired as i32;
            self.users
                .update_otp(rec)
                .await
                .map_err(|err| mfa_internal_status("verify_otp_expire_update", err.to_string()))?;
            return Ok(None);
        }
        if !authn::verify_otp_code(code, &self.otp_hash_key(), &rec.code_hash) {
            rec.attempt_count += 1;
            if authn::mfa_challenge::should_expire_after_failed_attempt(rec.attempt_count) {
                rec.status = authn_entity_pb::OtpStatus::Expired as i32;
            }
            self.users.update_otp(rec).await.map_err(|err| {
                mfa_internal_status("verify_otp_failed_attempt_update", err.to_string())
            })?;
            return Ok(None);
        }
        // Atomically flip PENDING→USED so two concurrent submissions of the same
        // OTP can't both verify (single-use even under a race). The loser sees the
        // row already consumed and is rejected.
        if !self
            .users
            .consume_otp_pending(otp_id, now)
            .await
            .map_err(|err| mfa_internal_status("verify_otp_consume_pending", err.to_string()))?
        {
            return Ok(None);
        }
        rec.status = authn_entity_pb::OtpStatus::Used as i32;
        rec.used_at_unix = now;
        Ok(Some(rec))
    }

    pub(super) async fn send_otp_impl(
        &self,
        request: Request<authn_pb::SendOtpRequest>,
    ) -> Result<Response<authn_pb::SendOtpResponse>, Status> {
        let req = request.into_inner();
        let user = self
            .users
            .get_user_by_id(&req.user_id)
            .await
            .map_err(|err| mfa_internal_status("send_otp_user_load", err.to_string()))?
            .ok_or_else(super::authn_user_not_found_status)?;
        let otp_type = if req.otp_type == authn_entity_pb::OtpType::Unspecified as i32 {
            authn_entity_pb::OtpType::SensitiveOperation as i32
        } else {
            req.otp_type
        };
        self.enforce_otp_cooldown(&user.user_id, otp_type, now_unix())
            .await?;
        let (otp_id, code) = self
            .issue_otp(&user, otp_type, req.correlation_id, now_unix())
            .await?;
        // Dev-only echo: in a non-production posture (UDB_OTP_DEV_ECHO=1) return
        // the plaintext code so conformance harnesses can complete the verify
        // flow without a delivery channel. Never populated in production.
        let dev_otp_code = if otp_dev_echo_enabled() {
            code.clone()
        } else {
            String::new()
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
                }),
            )
            .with_correlation(format!("otp_sent:{otp_id}"))
            .with_compliance(ComplianceEnvelope {
                actor: user.user_id.clone(),
                target_resource: user.user_id.clone(),
                operation: "otp_send".to_string(),
                outcome: "success".to_string(),
                reason_code: "otp_issued".to_string(),
                auth_method: "otp".to_string(),
                ..ComplianceEnvelope::default()
            }),
        )
        .await;
        Ok(Response::new(authn_pb::SendOtpResponse {
            otp_id,
            expires_in_seconds: self.config.otp_ttl_secs as i32,
            cooldown_seconds: self.config.otp_cooldown_secs as i32,
            dev_otp_code,
        }))
    }

    pub(super) async fn verify_otp_impl(
        &self,
        request: Request<authn_pb::VerifyOtpRequest>,
    ) -> Result<Response<authn_pb::VerifyOtpResponse>, Status> {
        let req = request.into_inner();
        Self::require_uuid_arg(&req.otp_id, "otp_id")?;
        let verified = self
            .verify_otp_record(&req.otp_id, &req.code, None, now_unix())
            .await?;
        if let Some(rec) = verified {
            let mut event_tenant_for_otp = String::new();
            if rec.otp_type == authn_entity_pb::OtpType::EmailVerification as i32 {
                if let Some(mut user) =
                    self.users
                        .get_user_by_id(&rec.user_id)
                        .await
                        .map_err(|err| {
                            mfa_internal_status("verify_otp_email_user_load", err.to_string())
                        })?
                {
                    user.status = crate::runtime::authn::AccountStatus::Active;
                    user.email_verified_at_unix = now_unix();
                    user.updated_at_unix = now_unix();
                    let event_email = user.email.clone();
                    let event_tenant = user.tenant_id.clone();
                    event_tenant_for_otp = event_tenant.clone();
                    self.users.put_user(user).await.map_err(|err| {
                        mfa_internal_status("verify_otp_email_user_update", err.to_string())
                    })?;
                    self.emit_event(
                        AuthEvent::new(
                            topics::EMAIL_VERIFIED,
                            rec.user_id.clone(),
                            event_tenant,
                            serde_json::json!({
                                "user_id": rec.user_id.clone(),
                                "email": event_email,
                            }),
                        )
                        .with_correlation(format!("email_verified:{}", rec.user_id))
                        .with_compliance(ComplianceEnvelope {
                            actor: rec.user_id.clone(),
                            target_resource: rec.user_id.clone(),
                            operation: "email_verify".to_string(),
                            outcome: "success".to_string(),
                            reason_code: "email_otp_verified".to_string(),
                            auth_method: "otp".to_string(),
                            ..ComplianceEnvelope::default()
                        }),
                    )
                    .await;
                }
            }
            if rec.otp_type == authn_entity_pb::OtpType::PhoneVerification as i32 {
                self.users
                    .mark_phone_verified(&rec.user_id, now_unix())
                    .await
                    .map_err(|err| {
                        mfa_internal_status("verify_otp_phone_mark_verified", err.to_string())
                    })?;
                self.emit_event(
                    AuthEvent::new(
                        topics::PHONE_VERIFIED,
                        rec.user_id.clone(),
                        rec.tenant_id.clone(),
                        serde_json::json!({
                            "user_id": rec.user_id.clone(),
                        }),
                    )
                    .with_correlation(format!("phone_verified:{}", rec.user_id))
                    .with_compliance(ComplianceEnvelope {
                        actor: rec.user_id.clone(),
                        target_resource: rec.user_id.clone(),
                        operation: "phone_verify".to_string(),
                        outcome: "success".to_string(),
                        reason_code: "phone_otp_verified".to_string(),
                        auth_method: "otp".to_string(),
                        ..ComplianceEnvelope::default()
                    }),
                )
                .await;
            }
            // OTP successfully verified (security-sensitive). `rec.tenant_id` is the
            // OTP owner's denormalized tenant; fall back to the email-branch tenant.
            let otp_tenant = if rec.tenant_id.trim().is_empty() {
                event_tenant_for_otp.clone()
            } else {
                rec.tenant_id.clone()
            };
            self.emit_event(
                AuthEvent::new(
                    topics::OTP_VERIFIED,
                    rec.user_id.clone(),
                    otp_tenant,
                    serde_json::json!({
                        "user_id": rec.user_id.clone(),
                        "otp_id": rec.otp_id.clone(),
                        "otp_type": rec.otp_type,
                    }),
                )
                .with_correlation(format!("otp_verify:{}", rec.otp_id))
                .with_compliance(ComplianceEnvelope {
                    actor: rec.user_id.clone(),
                    target_resource: rec.user_id.clone(),
                    operation: "otp_verify".to_string(),
                    outcome: "success".to_string(),
                    reason_code: "otp_verified".to_string(),
                    auth_method: "otp".to_string(),
                    ..ComplianceEnvelope::default()
                }),
            )
            .await;
            Ok(Response::new(authn_pb::VerifyOtpResponse {
                verified: true,
                user_id: rec.user_id,
                otp_type: rec.otp_type,
            }))
        } else {
            // Failed/expired/replayed OTP verification. Resolve the owning user +
            // tenant from the OTP id so the audit row is attributable; skip the
            // event only when the OTP id is unknown (no subject to attribute to).
            if let Some(otp) = self.users.get_otp(&req.otp_id).await.map_err(|err| {
                mfa_internal_status("verify_otp_failure_audit_lookup", err.to_string())
            })? {
                self.emit_event(
                    AuthEvent::new(
                        topics::OTP_FAILED,
                        otp.user_id.clone(),
                        otp.tenant_id.clone(),
                        serde_json::json!({
                            "user_id": otp.user_id.clone(),
                            "otp_id": req.otp_id.clone(),
                            "otp_type": otp.otp_type,
                        }),
                    )
                    .with_correlation(format!("otp_verify:{}", req.otp_id))
                    .with_compliance(ComplianceEnvelope {
                        actor: otp.user_id.clone(),
                        target_resource: otp.user_id.clone(),
                        operation: "otp_verify".to_string(),
                        outcome: "failure".to_string(),
                        reason_code: "otp_invalid_or_expired".to_string(),
                        auth_method: "otp".to_string(),
                        ..ComplianceEnvelope::default()
                    }),
                )
                .await;
            }
            Ok(Response::new(authn_pb::VerifyOtpResponse {
                verified: false,
                user_id: String::new(),
                otp_type: authn_entity_pb::OtpType::Unspecified as i32,
            }))
        }
    }

    pub(super) async fn resend_otp_impl(
        &self,
        request: Request<authn_pb::ResendOtpRequest>,
    ) -> Result<Response<authn_pb::ResendOtpResponse>, Status> {
        let req = request.into_inner();
        Self::require_uuid_arg(&req.original_otp_id, "otp_id")?;
        let mut original = self
            .users
            .get_otp(&req.original_otp_id)
            .await
            .map_err(|err| mfa_internal_status("resend_otp_original_load", err.to_string()))?
            .ok_or_else(super::authn_otp_not_found_status)?;
        let user = self
            .users
            .get_user_by_id(&original.user_id)
            .await
            .map_err(|err| mfa_internal_status("resend_otp_user_load", err.to_string()))?
            .ok_or_else(super::authn_user_not_found_status)?;
        let now = now_unix();
        self.enforce_otp_cooldown(&user.user_id, original.otp_type, now)
            .await?;
        let (otp_id, _code) = self
            .issue_otp(&user, original.otp_type, req.reason, now)
            .await?;
        original.status = authn_entity_pb::OtpStatus::Invalidated as i32;
        original.superseded_by_id = otp_id.clone();
        self.users
            .update_otp(original.clone())
            .await
            .map_err(|err| mfa_internal_status("resend_otp_supersede_update", err.to_string()))?;
        Ok(Response::new(authn_pb::ResendOtpResponse {
            otp_id,
            expires_in_seconds: self.config.otp_ttl_secs as i32,
            cooldown_seconds: self.config.otp_cooldown_secs as i32,
            attempts_remaining: authn::mfa_challenge::attempts_remaining(original.attempt_count),
        }))
    }

    pub(super) async fn enroll_mfa_impl(
        &self,
        request: Request<authn_pb::EnrollMfaRequest>,
    ) -> Result<Response<authn_pb::EnrollMfaResponse>, Status> {
        let req = request.into_inner();
        if req.mfa_type == authn_entity_pb::AuthFactorKind::Webauthn as i32 {
            return Err(mfa_webauthn_enrollment_rpc_required_status());
        }
        let mut user = self
            .users
            .get_user_by_id(&req.user_id)
            .await
            .map_err(|err| mfa_internal_status("enroll_mfa_user_load", err.to_string()))?
            .ok_or_else(super::authn_user_not_found_status)?;
        let now = now_unix();
        // RFC 6238 TOTP: mint a real base32 secret, store it encrypted-at-rest
        // as a *pending* enrollment (MFA stays disabled until the user proves
        // possession with a code), and hand the secret + provisioning URI back
        // once for the authenticator app to scan.
        let secret = authn::totp::generate_secret();
        let enc = authn::totp::encrypt_secret(&secret, &self.otp_hash_key()).ok_or_else(|| {
            mfa_internal_status("enroll_mfa_secret_encrypt", "failed to secure TOTP secret")
        })?;
        let uri = authn::totp::provisioning_uri("UDB", &user.username, &secret);
        user.totp_secret_hash = enc;
        user.updated_at_unix = now;
        let event_user_id = user.user_id.clone();
        let event_tenant = user.tenant_id.clone();
        self.users
            .put_user(user)
            .await
            .map_err(|err| mfa_internal_status("enroll_mfa_store", err.to_string()))?;
        // Pending TOTP enrollment started (security-sensitive; MFA is not yet
        // active until ConfirmMfaEnrollment proves possession of a code).
        self.emit_event(
            AuthEvent::new(
                topics::MFA_ENROLLED,
                event_user_id.clone(),
                event_tenant,
                serde_json::json!({
                    "user_id": event_user_id.clone(),
                    "factor_kind": req.mfa_type,
                    "state": "pending",
                }),
            )
            .with_correlation(format!("mfa_enroll:{event_user_id}"))
            .with_compliance(ComplianceEnvelope {
                actor: event_user_id.clone(),
                target_resource: event_user_id.clone(),
                operation: "mfa_enroll".to_string(),
                outcome: "success".to_string(),
                reason_code: "totp_enrollment_started".to_string(),
                auth_method: "mfa_totp".to_string(),
                ..ComplianceEnvelope::default()
            }),
        )
        .await;
        Ok(Response::new(authn_pb::EnrollMfaResponse {
            totp_secret: secret,
            totp_qr_uri: uri,
            // TOTP enrollment is confirmed by presenting a code, not a server
            // OTP id; the field stays empty for the TOTP factor.
            verify_otp_id: String::new(),
        }))
    }

    pub(super) async fn confirm_mfa_enrollment_impl(
        &self,
        request: Request<authn_pb::ConfirmMfaEnrollmentRequest>,
    ) -> Result<Response<authn_pb::ConfirmMfaEnrollmentResponse>, Status> {
        let req = request.into_inner();
        let now = now_unix();
        let mut user = self
            .users
            .get_user_by_id(&req.user_id)
            .await
            .map_err(|err| mfa_internal_status("confirm_mfa_user_load", err.to_string()))?
            .ok_or_else(super::authn_user_not_found_status)?;
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
        let event_user_id = user.user_id.clone();
        let event_tenant = user.tenant_id.clone();
        self.users
            .put_user(user)
            .await
            .map_err(|err| mfa_internal_status("confirm_mfa_store", err.to_string()))?;
        // MFA second factor activated (the user proved possession of a TOTP code).
        self.emit_event(
            AuthEvent::new(
                topics::MFA_CHANGED,
                event_user_id.clone(),
                event_tenant,
                serde_json::json!({
                    "user_id": event_user_id.clone(),
                    "factor_kind": authn_entity_pb::AuthFactorKind::Totp as i32,
                    "state": "enabled",
                }),
            )
            .with_correlation(format!("mfa_confirm:{event_user_id}"))
            .with_compliance(ComplianceEnvelope {
                actor: event_user_id.clone(),
                target_resource: event_user_id.clone(),
                operation: "mfa_enable".to_string(),
                outcome: "success".to_string(),
                reason_code: "totp_enrollment_confirmed".to_string(),
                auth_method: "mfa_totp".to_string(),
                ..ComplianceEnvelope::default()
            }),
        )
        .await;
        Ok(Response::new(authn_pb::ConfirmMfaEnrollmentResponse {
            enrolled: true,
        }))
    }

    pub(super) async fn generate_recovery_codes_impl(
        &self,
        request: Request<authn_pb::GenerateRecoveryCodesRequest>,
    ) -> Result<Response<authn_pb::GenerateRecoveryCodesResponse>, Status> {
        let req = request.into_inner();
        // The user must exist; recovery codes are an alternative MFA second factor.
        let user = self
            .users
            .get_user_by_id(&req.user_id)
            .await
            .map_err(|err| mfa_internal_status("recovery_codes_user_load", err.to_string()))?
            .ok_or_else(super::authn_user_not_found_status)?;
        let count = if req.count <= 0 {
            10
        } else {
            req.count.min(20)
        } as usize;
        let mut codes = Vec::with_capacity(count);
        let mut hashes = Vec::with_capacity(count);
        for _ in 0..count {
            let code = authn::mfa_challenge::generate_recovery_code();
            hashes.push(authn::hash_recovery_code(&code, &self.otp_hash_key()));
            codes.push(code);
        }
        // Replacing the set invalidates any previously-issued codes. Denormalize
        // the owning user's tenant so the rows are tenant-scoped under RLS.
        self.users
            .replace_recovery_codes(&req.user_id, &user.tenant_id, &hashes)
            .await
            .map_err(|err| mfa_internal_status("recovery_codes_replace", err.to_string()))?;
        // Audit the regeneration (the plaintext codes NEVER enter the body — only
        // the count). Replacing the set invalidates any prior codes.
        self.emit_event(
            AuthEvent::new(
                topics::RECOVERY_CODES_GENERATED,
                req.user_id.clone(),
                user.tenant_id.clone(),
                serde_json::json!({
                    "user_id": req.user_id.clone(),
                    "generated_count": codes.len(),
                }),
            )
            .with_correlation(format!("recovery_codes:{}", req.user_id))
            .with_compliance(ComplianceEnvelope {
                actor: req.user_id.clone(),
                target_resource: req.user_id.clone(),
                operation: "recovery_codes_generate".to_string(),
                outcome: "success".to_string(),
                reason_code: "recovery_codes_regenerated".to_string(),
                auth_method: "mfa".to_string(),
                ..ComplianceEnvelope::default()
            }),
        )
        .await;
        Ok(Response::new(authn_pb::GenerateRecoveryCodesResponse {
            generated: codes.len() as i32,
            codes,
        }))
    }

    pub(super) async fn put_mfa_policy_impl(
        &self,
        request: Request<authn_pb::PutMfaPolicyRequest>,
    ) -> Result<Response<authn_pb::PutMfaPolicyResponse>, Status> {
        let req = request.into_inner();
        if req.tenant_id.trim().is_empty() {
            return Err(mfa_required_field(
                "tenant_id",
                "must be a non-empty tenant id",
                "tenant_id is required",
            ));
        }
        self.users
            .put_mfa_policy(&req.tenant_id, req.require_mfa)
            .await
            .map_err(|err| mfa_internal_status("put_mfa_policy", err.to_string()))?;
        Ok(Response::new(authn_pb::PutMfaPolicyResponse {
            tenant_id: req.tenant_id,
            require_mfa: req.require_mfa,
        }))
    }

    pub(super) async fn get_mfa_policy_impl(
        &self,
        request: Request<authn_pb::GetMfaPolicyRequest>,
    ) -> Result<Response<authn_pb::GetMfaPolicyResponse>, Status> {
        let req = request.into_inner();
        if req.tenant_id.trim().is_empty() {
            return Err(mfa_required_field(
                "tenant_id",
                "must be a non-empty tenant id",
                "tenant_id is required",
            ));
        }
        let require_mfa = self
            .users
            .tenant_requires_mfa(&req.tenant_id)
            .await
            .map_err(|err| mfa_internal_status("get_mfa_policy", err.to_string()))?;
        Ok(Response::new(authn_pb::GetMfaPolicyResponse {
            tenant_id: req.tenant_id,
            require_mfa,
        }))
    }

    pub(super) async fn send_phone_verification_impl(
        &self,
        request: Request<authn_pb::SendPhoneVerificationRequest>,
    ) -> Result<Response<authn_pb::SendPhoneVerificationResponse>, Status> {
        let req = request.into_inner();
        let phone = req.phone.trim().to_string();
        if phone.is_empty() {
            return Err(mfa_required_field(
                "phone",
                "must be a non-empty phone number",
                "phone is required",
            ));
        }
        if phone.chars().count() > 32 {
            return Err(mfa_required_field(
                "phone",
                "must be at most 32 characters (E.164 format)",
                "phone must be at most 32 characters (E.164 format)",
            ));
        }
        let user = self
            .users
            .get_user_by_id(&req.user_id)
            .await
            .map_err(|err| {
                mfa_internal_status("send_phone_verification_user_load", err.to_string())
            })?
            .ok_or_else(super::authn_user_not_found_status)?;
        // Record the number (clearing any prior verification), then SMS an OTP.
        self.users
            .set_user_phone(&user.user_id, &phone)
            .await
            .map_err(|err| {
                mfa_internal_status("send_phone_verification_set_phone", err.to_string())
            })?;
        let now = now_unix();
        self.enforce_otp_cooldown(
            &user.user_id,
            authn_entity_pb::OtpType::PhoneVerification as i32,
            now,
        )
        .await?;
        let (otp_id, code) = self
            .issue_otp_to(
                &user,
                authn_entity_pb::OtpType::PhoneVerification as i32,
                "sms",
                &phone,
                format!("phone_verification:{}", user.user_id),
                now,
            )
            .await?;
        // Dev-only echo of the phone OTP under the single fail-closed gate
        // (empty in production / when the env opt-in is unset).
        let dev_otp_code = if otp_dev_echo_enabled() {
            code
        } else {
            String::new()
        };
        Ok(Response::new(authn_pb::SendPhoneVerificationResponse {
            otp_id,
            dev_otp_code,
        }))
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
    use prost::Message as _;
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

    fn assert_validation_field(status: &Status, field: &str, description: &str) {
        assert_eq!(status.code(), Code::InvalidArgument);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, field);
        assert_eq!(detail.field_violations[0].description, description);
    }

    fn assert_policy_detail(
        status: &Status,
        operation: &str,
        policy_decision_id: &str,
        message: &str,
    ) {
        assert_eq!(status.code(), Code::FailedPrecondition);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Policy as i32);
        assert_eq!(detail.operation, operation);
        assert_eq!(detail.policy_decision_id, policy_decision_id);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
    }

    fn assert_internal_detail(status: &Status, operation: &str, message: &str) {
        assert_eq!(status.code(), Code::Internal);
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
    fn mfa_internal_status_carries_typed_detail() {
        let status = mfa_internal_status("verify_otp_load", "otp store failed");
        assert_internal_detail(&status, "verify_otp_load", "otp store failed");
    }

    #[test]
    fn webauthn_mfa_enrollment_policy_carries_typed_detail() {
        assert_policy_detail(
            &mfa_webauthn_enrollment_rpc_required_status(),
            "mfa_enrollment",
            "webauthn_enrollment_rpc_required",
            "WebAuthn enrollment uses StartWebAuthnRegistration and FinishWebAuthnRegistration",
        );
    }

    #[tokio::test]
    async fn put_mfa_policy_missing_tenant_id_carries_field_violation() {
        let err = svc()
            .put_mfa_policy(Request::new(authn_pb::PutMfaPolicyRequest {
                tenant_id: " ".to_string(),
                ..Default::default()
            }))
            .await
            .expect_err("missing tenant_id must fail before store access");

        assert_eq!(err.message(), "tenant_id is required");
        assert_validation_field(&err, "tenant_id", "must be a non-empty tenant id");
    }

    #[tokio::test]
    async fn get_mfa_policy_missing_tenant_id_carries_field_violation() {
        let err = svc()
            .get_mfa_policy(Request::new(authn_pb::GetMfaPolicyRequest {
                tenant_id: " ".to_string(),
                ..Default::default()
            }))
            .await
            .expect_err("missing tenant_id must fail before store access");

        assert_eq!(err.message(), "tenant_id is required");
        assert_validation_field(&err, "tenant_id", "must be a non-empty tenant id");
    }

    #[tokio::test]
    async fn send_phone_verification_missing_phone_carries_field_violation() {
        let err = svc()
            .send_phone_verification(Request::new(authn_pb::SendPhoneVerificationRequest {
                phone: " ".to_string(),
                ..Default::default()
            }))
            .await
            .expect_err("missing phone must fail before user lookup");

        assert_eq!(err.message(), "phone is required");
        assert_validation_field(&err, "phone", "must be a non-empty phone number");
    }

    #[tokio::test]
    async fn send_phone_verification_long_phone_carries_field_violation() {
        let err = svc()
            .send_phone_verification(Request::new(authn_pb::SendPhoneVerificationRequest {
                phone: "1".repeat(33),
                ..Default::default()
            }))
            .await
            .expect_err("invalid phone length must fail before user lookup");

        assert_eq!(
            err.message(),
            "phone must be at most 32 characters (E.164 format)"
        );
        assert_validation_field(
            &err,
            "phone",
            "must be at most 32 characters (E.164 format)",
        );
    }
}
