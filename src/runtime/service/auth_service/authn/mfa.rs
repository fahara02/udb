//! Multi-factor auth: email/SMS OTP issuance + verification, TOTP enrollment,
//! recovery codes, the per-tenant MFA-enforcement policy, and phone verification.

use super::*;

fn generated_code(_seed: &str, _now: u64) -> String {
    // Draw the 6-digit code from the CSPRNG-backed UUID generator rather than a
    // deterministic time+seed HMAC fold. The `u128 % 1_000_000` modulo bias is
    // negligible (2^128 ≫ 10^6). The code is hashed before storage, so it never
    // needs to be recomputable.
    let n = (Uuid::new_v4().as_u128() % 1_000_000) as u32;
    format!("{n:06}")
}

/// A CSPRNG-backed, human-transcribable single-use recovery code (three groups
/// of four lowercase-hex chars, e.g. `a1b2-c3d4-e5f6`; ~48 bits of entropy).
/// Only its keyed hash is stored, so it need not be recomputable.
fn generate_recovery_code() -> String {
    let raw = Uuid::new_v4().simple().to_string();
    format!("{}-{}-{}", &raw[0..4], &raw[4..8], &raw[8..12])
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
        let otp_id = Uuid::new_v4().to_string();
        let code = generated_code(&format!("{otp_id}:{}", user.user_id), now);
        let rec = OtpRecord {
            otp_id: otp_id.clone(),
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
        };
        self.users.put_otp(rec).await.map_err(Status::internal)?;
        // Best-effort outbound delivery to the operator's channel gateway. A
        // delivery failure (or no configured webhook) never fails issuance — the
        // OTP is persisted and the caller holds the otp_id.
        authn::deliver_otp(channel, address, &code, otp_type, &user.user_id).await;
        #[cfg(test)]
        if let Ok(mut codes) = test_otp_codes().lock() {
            codes.insert(otp_id.clone(), code.clone());
        }
        Ok((otp_id, code))
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
            .map_err(Status::internal)?
        {
            let ready_at = last.saturating_add(cooldown);
            if now < ready_at {
                return Err(Status::resource_exhausted(format!(
                    "OTP cooldown active; retry in {} seconds",
                    ready_at - now
                )));
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
        // Atomically flip PENDING→USED so two concurrent submissions of the same
        // OTP can't both verify (single-use even under a race). The loser sees the
        // row already consumed and is rejected.
        if !self
            .users
            .consume_otp_pending(otp_id, now)
            .await
            .map_err(Status::internal)?
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
            .map_err(Status::internal)?
            .ok_or_else(|| Status::not_found("user not found"))?;
        let otp_type = if req.otp_type == authn_entity_pb::OtpType::Unspecified as i32 {
            authn_entity_pb::OtpType::SensitiveOperation as i32
        } else {
            req.otp_type
        };
        self.enforce_otp_cooldown(&user.user_id, otp_type, now_unix())
            .await?;
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

    pub(super) async fn verify_otp_impl(
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
            if rec.otp_type == authn_entity_pb::OtpType::PhoneVerification as i32 {
                self.users
                    .mark_phone_verified(&rec.user_id, now_unix())
                    .await
                    .map_err(Status::internal)?;
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

    pub(super) async fn resend_otp_impl(
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
            .map_err(Status::internal)?;
        Ok(Response::new(authn_pb::ResendOtpResponse {
            otp_id,
            expires_in_seconds: self.config.otp_ttl_secs as i32,
            cooldown_seconds: self.config.otp_cooldown_secs as i32,
            attempts_remaining: (5 - original.attempt_count).max(0),
        }))
    }

    pub(super) async fn enroll_mfa_impl(
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

    pub(super) async fn generate_recovery_codes_impl(
        &self,
        request: Request<authn_pb::GenerateRecoveryCodesRequest>,
    ) -> Result<Response<authn_pb::GenerateRecoveryCodesResponse>, Status> {
        let req = request.into_inner();
        // The user must exist; recovery codes are an alternative MFA second factor.
        self.users
            .get_user_by_id(&req.user_id)
            .await
            .map_err(Status::internal)?
            .ok_or_else(|| Status::not_found("user not found"))?;
        let count = if req.count <= 0 {
            10
        } else {
            req.count.min(20)
        } as usize;
        let mut codes = Vec::with_capacity(count);
        let mut hashes = Vec::with_capacity(count);
        for _ in 0..count {
            let code = generate_recovery_code();
            hashes.push(authn::hash_recovery_code(&code, &self.otp_hash_key()));
            codes.push(code);
        }
        // Replacing the set invalidates any previously-issued codes.
        self.users
            .replace_recovery_codes(&req.user_id, &hashes)
            .await
            .map_err(Status::internal)?;
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
            return Err(Status::invalid_argument("tenant_id is required"));
        }
        self.users
            .put_mfa_policy(&req.tenant_id, req.require_mfa)
            .await
            .map_err(Status::internal)?;
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
            return Err(Status::invalid_argument("tenant_id is required"));
        }
        let require_mfa = self
            .users
            .tenant_requires_mfa(&req.tenant_id)
            .await
            .map_err(Status::internal)?;
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
            return Err(Status::invalid_argument("phone is required"));
        }
        if phone.chars().count() > 32 {
            return Err(Status::invalid_argument(
                "phone must be at most 32 characters (E.164 format)",
            ));
        }
        let user = self
            .users
            .get_user_by_id(&req.user_id)
            .await
            .map_err(Status::internal)?
            .ok_or_else(|| Status::not_found("user not found"))?;
        // Record the number (clearing any prior verification), then SMS an OTP.
        self.users
            .set_user_phone(&user.user_id, &phone)
            .await
            .map_err(Status::internal)?;
        let now = now_unix();
        self.enforce_otp_cooldown(
            &user.user_id,
            authn_entity_pb::OtpType::PhoneVerification as i32,
            now,
        )
        .await?;
        let (otp_id, _code) = self
            .issue_otp_to(
                &user,
                authn_entity_pb::OtpType::PhoneVerification as i32,
                "sms",
                &phone,
                format!("phone_verification:{}", user.user_id),
                now,
            )
            .await?;
        Ok(Response::new(authn_pb::SendPhoneVerificationResponse {
            otp_id,
        }))
    }
}
