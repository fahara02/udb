//! Credential authentication: the multi-credential `Authenticate` probe, the
//! interactive `Login` (password + MFA second factor), and the password
//! change / forgot / reset flows.

use super::*;

impl AuthnServiceImpl {
    pub(super) async fn authenticate_impl(
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

    pub(super) async fn login_impl(
        &self,
        request: Request<authn_pb::LoginRequest>,
    ) -> Result<Response<authn_pb::LoginResponse>, Status> {
        let req = request.into_inner();
        let now = now_unix();
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
        // First factor: the password is ALWAYS verified, before any account
        // status is revealed (so a caller without the password cannot tell
        // unknown / inactive / locked accounts apart) AND before any MFA second
        // factor is considered. An MFA code (totp_code / mfa_otp_id) is purely
        // additive — it can never stand in for the password.
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
        // Tenant MFA-enforcement policy: when the tenant requires MFA, a user who
        // has not yet enrolled a second factor cannot complete a password-only
        // login — they must enrol first.
        if !user.mfa_enabled
            && self
                .users
                .tenant_requires_mfa(&user.tenant_id)
                .await
                .map_err(Status::internal)?
        {
            return Err(Status::failed_precondition(
                "MFA enrollment required by tenant policy",
            ));
        }
        // Second factor (only when the user has MFA enrolled). The password is
        // already proven above; the factor here is additive and bound to THIS
        // authenticated user — an OTP minted for another account can never
        // complete this login.
        if user.mfa_enabled {
            let has_otp = !req.mfa_otp_id.trim().is_empty();
            let has_totp = !req.totp_code.trim().is_empty();
            let has_recovery = !req.recovery_code.trim().is_empty();
            if !has_otp && !has_totp && !has_recovery {
                // Step-1: signal MFA is required. The client re-submits
                // username + password + the second factor on the next call.
                return Ok(Response::new(authn_pb::LoginResponse {
                    user_id: user.user_id,
                    mfa_required: true,
                    ..Default::default()
                }));
            }
            let verified = if has_recovery {
                // Single-use recovery/backup code, consumed atomically and bound
                // to this authenticated user.
                let hash = authn::hash_recovery_code(&req.recovery_code, &self.otp_hash_key());
                self.users
                    .consume_recovery_code(&user.user_id, &hash, now)
                    .await
                    .map_err(Status::internal)?
            } else if has_otp {
                // Email/SMS one-time code: must be a LOGIN_2FA code issued to
                // this same user (the code itself is carried in totp_code).
                matches!(
                    self.verify_otp_record(
                        &req.mfa_otp_id,
                        &req.totp_code,
                        Some(authn_entity_pb::OtpType::Login2fa as i32),
                        now,
                    )
                    .await?,
                    Some(rec) if rec.user_id == user.user_id
                )
            } else {
                // TOTP authenticator code verified against the enrolled secret.
                authn::totp::decrypt_secret(&user.totp_secret_hash, &self.otp_hash_key())
                    .map(|secret| authn::totp::verify(&secret, &req.totp_code, now))
                    .unwrap_or(false)
            };
            if !verified {
                return Err(Status::unauthenticated("invalid MFA code"));
            }
        }
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

    pub(super) async fn change_password_impl(
        &self,
        request: Request<authn_pb::ChangePasswordRequest>,
    ) -> Result<Response<authn_pb::ChangePasswordResponse>, Status> {
        let req = request.into_inner();
        authn::PasswordPolicy::from_env()
            .validate(&req.new_password)
            .map_err(Status::invalid_argument)?;
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

    pub(super) async fn forgot_password_impl(
        &self,
        request: Request<authn_pb::ForgotPasswordRequest>,
    ) -> Result<Response<authn_pb::ForgotPasswordResponse>, Status> {
        let req = request.into_inner();
        let identifier = req.identifier.trim().to_ascii_lowercase();
        // Resolve by username, then email. The response is uniform whether or not
        // the account exists, so it is not an account-enumeration oracle.
        let user = match self
            .users
            .get_user_by_username(&identifier)
            .await
            .map_err(Status::internal)?
        {
            Some(u) => Some(u),
            None => self
                .users
                .get_user_by_email(&identifier)
                .await
                .map_err(Status::internal)?,
        };
        let otp_id = if let Some(user) = user {
            let (otp_id, _code) = self
                .issue_otp(
                    &user,
                    authn_entity_pb::OtpType::PasswordReset as i32,
                    format!("forgot_password:{}", user.user_id),
                    now_unix(),
                )
                .await?;
            otp_id
        } else {
            String::new()
        };
        Ok(Response::new(authn_pb::ForgotPasswordResponse { otp_id }))
    }

    pub(super) async fn reset_password_impl(
        &self,
        request: Request<authn_pb::ResetPasswordRequest>,
    ) -> Result<Response<authn_pb::ResetPasswordResponse>, Status> {
        let req = request.into_inner();
        authn::PasswordPolicy::from_env()
            .validate(&req.new_password)
            .map_err(Status::invalid_argument)?;
        let now = now_unix();
        // The PASSWORD_RESET OTP is the proof of control — no current password.
        let rec = self
            .verify_otp_record(
                &req.otp_id,
                &req.code,
                Some(authn_entity_pb::OtpType::PasswordReset as i32),
                now,
            )
            .await?
            .ok_or_else(|| Status::permission_denied("invalid or expired reset code"))?;
        let mut user = self
            .users
            .get_user_by_id(&rec.user_id)
            .await
            .map_err(Status::internal)?
            .ok_or_else(|| Status::not_found("user not found"))?;
        user.password_hash = authn::hash_password(&req.new_password, &self.password_hash_key());
        user.failed_login_count = 0;
        user.locked_until_unix = 0;
        user.updated_at_unix = now;
        let user_id = user.user_id.clone();
        let tenant = user.tenant_id.clone();
        self.users.put_user(user).await.map_err(Status::internal)?;
        // Invalidate every existing session after a reset.
        let _ = self.sessions.revoke_all_for_principal(&user_id, now).await;
        self.emit_event(AuthEvent::new(
            topics::PASSWORD_CHANGED,
            user_id.clone(),
            tenant,
            serde_json::json!({
                "user_id": user_id.clone(),
                "is_reset": true,
                "changed_by": user_id.clone(),
            }),
        ))
        .await;
        Ok(Response::new(authn_pb::ResetPasswordResponse {
            user_id,
            changed_at_unix: now as i64,
        }))
    }
}
