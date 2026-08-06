//! Credential authentication: the multi-credential `Authenticate` probe, the
//! interactive `Login` (password + MFA second factor), and the password
//! change / forgot / reset flows.

use super::*;

fn login_invalid_fields<I, F, D>(message: impl Into<String>, fields: I) -> Status
where
    I: IntoIterator<Item = (F, D)>,
    F: Into<String>,
    D: Into<String>,
{
    crate::runtime::executor_utils::invalid_argument_fields(message, fields)
}

fn oidc_feature_required_status() -> Status {
    authn_capability_status(
        "oidc_authentication",
        "oidc_feature",
        "OIDC authentication requires building UDB with the `oidc` feature",
    )
}

#[derive(Debug, Clone, Default)]
struct PasswordLoginGrants {
    scopes: Vec<String>,
    roles: Vec<String>,
    service_identity: String,
}

fn requested_scopes_from_metadata(metadata: &tonic::metadata::MetadataMap) -> Vec<String> {
    metadata
        .get("x-scopes")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|scope| !scope.is_empty())
        .map(ToString::to_string)
        .fold(Vec::new(), |mut acc, scope| {
            if !acc.contains(&scope) {
                acc.push(scope);
            }
            acc
        })
}

fn service_scope_policy_status(message: &'static str) -> Status {
    login_policy_status_with_code("password_login", "service_account_scope_grant", message)
}
fn tenant_mfa_enrollment_required_status() -> Status {
    crate::runtime::executor_utils::policy_status(
        "password_login",
        "tenant_mfa_enrollment_required",
        "MFA enrollment required by tenant policy",
    )
}

fn login_policy_status_with_code(
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

fn login_internal_status(operation: impl Into<String>, message: impl Into<String>) -> Status {
    crate::runtime::executor_utils::internal_status("authn", operation, message)
}

fn password_login_user_active_status() -> Status {
    login_policy_status_with_code("password_login", "user_not_active", "user is not active")
}

fn password_change_otp_verified_status() -> Status {
    login_policy_status_with_code(
        "change_password",
        "password_change_otp_verified",
        "password-change OTP is not verified",
    )
}

fn reset_password_request_valid_status() -> Status {
    login_policy_status_with_code(
        "reset_password",
        "reset_request_valid",
        "invalid reset request",
    )
}

impl AuthnServiceImpl {
    /// Service-account password login resolves only through the typed durable
    /// grant. Legacy profile attributes are migration input, never a parallel
    /// runtime authority.
    fn resolve_password_login_grants_with_typed(
        &self,
        user: &UserRecord,
        requested_scopes: &[String],
        typed_grant: Option<&super::super::grants::GrantRecord>,
    ) -> Result<PasswordLoginGrants, Status> {
        if user.account_kind != authn::AccountKind::ServiceAccount {
            let (scopes, roles) =
                self.resolve_effective_grants(&user.user_id, &user.tenant_id, &user.project_id);
            return Ok(PasswordLoginGrants {
                scopes,
                roles,
                service_identity: String::new(),
            });
        }

        if let Some(grant) = typed_grant {
            if !grant.status.eq_ignore_ascii_case("ACTIVE") {
                return Err(service_scope_policy_status(
                    "service account grant is revoked",
                ));
            }
            if grant.tenant_id.trim() != user.tenant_id.trim()
                || grant.project_id.trim() != user.project_id.trim()
                || grant.service_identity.trim().is_empty()
            {
                return Err(service_scope_policy_status(
                    "service account grant lineage does not match the active account",
                ));
            }
            let scopes =
                super::super::grants::validate_service_scopes(requested_scopes, &grant.scopes)?;
            return Ok(PasswordLoginGrants {
                scopes,
                roles: Vec::new(),
                service_identity: grant.service_identity.clone(),
            });
        }

        Err(service_scope_policy_status(
            "service account has no active typed grant; run `udb auth migrate-grants` or create a grant",
        ))
    }
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
            .map_err(|err| login_internal_status("authenticate_api_key", err.to_string()))?
            .ok_or_else(|| {
                crate::runtime::executor_utils::unauthenticated_status(
                    "invalid_credential",
                    "invalid credential",
                )
            })?;
            // CRIT-4: attenuate the key's stored scopes against the owner's
            // CURRENT typed grant — the same live check the data-plane
            // credential layer applies. A revoked/replaced grant or inactive
            // owner invalidates the key here too; the exchanged bearer can
            // never carry scopes wider than the live grant.
            let rec = {
                let mut rec = rec;
                let pool = self.require_pool()?;
                match super::super::grants::attenuate_key_scopes_against_grant(
                    pool,
                    &rec.tenant_id,
                    &rec.principal_id,
                    rec.grant_revision,
                    &rec.scopes,
                )
                .await
                {
                    Ok(Some(effective)) => rec.scopes = effective,
                    Ok(None) => {
                        return Err(crate::runtime::executor_utils::unauthenticated_status(
                            "invalid_credential",
                            "invalid credential",
                        ));
                    }
                    Err(error) => {
                        return Err(login_internal_status("authenticate_api_key", error));
                    }
                }
                rec
            };
            let mut principal = principal_from_api_key(&rec);
            let mut requested_scopes = req.requested_scopes.clone();
            requested_scopes.retain(|scope| !scope.trim().is_empty());
            requested_scopes.dedup();
            let token_scopes = if requested_scopes.is_empty() {
                rec.scopes.clone()
            } else {
                for requested in &requested_scopes {
                    if !rec.scopes.contains(requested) {
                        return Err(crate::runtime::executor_utils::policy_status_with_code(
                            tonic::Code::PermissionDenied,
                            "login",
                            "api_key_scope_not_granted",
                            "requested API-key scope is not granted to this key",
                        ));
                    }
                }
                requested_scopes
            };
            principal.scopes = token_scopes.clone();
            let (access_token, access_exp) = self.issue_access_token(
                &rec.principal_id,
                &rec.tenant_id,
                &rec.project_id,
                &token_scopes,
                &[],
                &rec.service_identity,
                &format!("apikey_{}", rec.key_prefix),
                "api_key",
                authn_entity_pb::AccountKind::ServiceAccount as i32,
                now,
            );
            let expires_at_unix = if access_exp > 0 {
                access_exp
            } else {
                rec.expires_at_unix as i64
            };
            return Ok(Response::new(authn_pb::AuthnResponse {
                principal: Some(authn_principal_to_pb(
                    &principal,
                    expires_at_unix,
                    authn_entity_pb::AccountKind::ServiceAccount as i32,
                )),
                session_id: String::new(),
                access_token,
                expires_at_unix,
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
            .map_err(|err| login_internal_status("authenticate_session", err.to_string()))?
            .ok_or_else(|| {
                crate::runtime::executor_utils::unauthenticated_status(
                    "invalid_credential",
                    "invalid credential",
                )
            })?;
            let principal = principal_from_session(&rec);
            return Ok(Response::new(authn_pb::AuthnResponse {
                principal: Some(authn_principal_to_pb(
                    &principal,
                    rec.expires_at_unix as i64,
                    // A server-side session record does not persist the account
                    // classification, so it stays UNSPECIFIED on this path.
                    authn_entity_pb::AccountKind::Unspecified as i32,
                )),
                session_id: String::new(),
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
                    principal: Some(authn_principal_to_pb(
                        &principal,
                        0,
                        authn_entity_pb::AccountKind::Unspecified as i32,
                    )),
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
                return Err(oidc_feature_required_status());
            }
        }

        // Hybrid external provider: verify a signed JWT first, then map the
        // verified claims to a UDB principal. Raw JSON "claims" are not accepted
        // here; trusted PEPs must pass a bearer token validated by UDB.
        if !req.external_provider_id.trim().is_empty() {
            if req.external_token.matches('.').count() != 2 {
                return Err(crate::runtime::executor_utils::unauthenticated_status(
                    "invalid_credential",
                    "invalid credential",
                ));
            }
            let claims =
                validate_bearer_token(&self.security, &req.external_token).map_err(|_| {
                    crate::runtime::executor_utils::unauthenticated_status(
                        "invalid_credential",
                        "invalid credential",
                    )
                })?;
            if !req.issuer.trim().is_empty()
                && claims.iss.as_deref().unwrap_or_default() != req.issuer
            {
                return Err(crate::runtime::executor_utils::unauthenticated_status(
                    "invalid_credential",
                    "invalid credential",
                ));
            }
            if !req.audience.trim().is_empty()
                && self.security.jwt_audience.as_deref() != Some(req.audience.as_str())
            {
                return Err(crate::runtime::executor_utils::unauthenticated_status(
                    "invalid_credential",
                    "invalid credential",
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
                .map_err(|_| {
                    crate::runtime::executor_utils::unauthenticated_status(
                        "invalid_credential",
                        "invalid credential",
                    )
                })?;
            return Ok(Response::new(authn_pb::AuthnResponse {
                principal: Some(authn_principal_to_pb(
                    &principal,
                    0,
                    // External-IdP mapping does not classify the account kind;
                    // UDB authz still governs access regardless.
                    authn_entity_pb::AccountKind::Unspecified as i32,
                )),
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
            let claims = validate_bearer_token(&self.security, &req.bearer_token).map_err(|e| {
                crate::runtime::executor_utils::unauthenticated_status(
                    "bearer_verification_failed",
                    e.to_string(),
                )
            })?;
            // Phase 3: a UDB-issued JWT must still be backed by live persisted
            // state — reject it here too if the subject user was suspended or the
            // issuing session revoked (same rule as ValidateToken).
            if !self.jwt_persisted_state_valid(&claims, now_unix()).await? {
                return Err(crate::runtime::executor_utils::unauthenticated_status(
                    "subject_inactive",
                    "token subject is no longer active",
                ));
            }
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
                principal: Some(authn_principal_to_pb(
                    &principal,
                    0,
                    // Carry the account classification the login minted into the
                    // token so a service account stays assertable as one here —
                    // this is the path the enterprise SDKs read after login.
                    claims.account_kind.unwrap_or(0),
                )),
                session_id: String::new(),
                access_token: String::new(),
                expires_at_unix: 0,
                relationship_version: claims.relationships_version.unwrap_or_default(),
                warnings: Vec::new(),
            }));
        }

        Err(login_invalid_fields(
            "no credential supplied (set api_key, session_id, bearer_token, or external_provider_id+external_token)",
            [
                ("api_key", "must include one supported credential"),
                ("session_id", "must include one supported credential"),
                ("bearer_token", "must include one supported credential"),
                ("external_token", "must include one supported credential"),
            ],
        ))
    }

    pub(super) async fn login_impl(
        &self,
        request: Request<authn_pb::LoginRequest>,
    ) -> Result<Response<authn_pb::LoginResponse>, Status> {
        let requested_scopes = requested_scopes_from_metadata(request.metadata());
        let req = request.into_inner();
        let now = now_unix();
        let login_name = req.username.to_ascii_lowercase();
        let mut user = match self
            .users
            .get_user_by_username(&login_name)
            .await
            .map_err(|err| login_internal_status("password_login_user_lookup", err.to_string()))?
        {
            Some(user) => user,
            None => self
                .users
                .get_user_by_email(&login_name)
                .await
                .map_err(|err| {
                    login_internal_status("password_login_email_lookup", err.to_string())
                })?
                .ok_or_else(|| {
                    crate::runtime::executor_utils::unauthenticated_status(
                        "invalid_login",
                        "invalid username or password",
                    )
                })?,
        };
        // Brute-force lockout: reject while locked, using the same opaque
        // error as a bad password so the lock state is not an enumeration
        // oracle.
        const MAX_FAILED_LOGINS: i32 = 5;
        const LOCKOUT_SECONDS: u64 = 15 * 60;
        if user.locked_until_unix > now {
            self.metrics.record_auth_login(false);
            return Err(crate::runtime::executor_utils::unauthenticated_status(
                "invalid_login",
                "invalid username or password",
            ));
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
            let event_ua = req.user_agent.clone();
            self.users.put_user(user).await.map_err(|err| {
                login_internal_status("password_login_failed_attempt_update", err.to_string())
            })?;
            // Audit the failed credential attempt (security-sensitive).
            self.emit_event(
                AuthEvent::new(
                    topics::LOGIN_FAILED,
                    event_user_id.clone(),
                    event_tenant.clone(),
                    serde_json::json!({
                        "user_id": event_user_id.clone(),
                        "attempt_count": attempt_count,
                        "ip_address": event_ip.clone(),
                        "locked": now_locked,
                    }),
                )
                .with_correlation(format!("login_failed:{event_user_id}"))
                .with_compliance(ComplianceEnvelope {
                    actor: event_user_id.clone(),
                    target_resource: event_user_id.clone(),
                    operation: "login".to_string(),
                    outcome: "failure".to_string(),
                    reason_code: "invalid_password".to_string(),
                    auth_method: "password".to_string(),
                    source_ip: event_ip.clone(),
                    user_agent: event_ua.clone(),
                    ..ComplianceEnvelope::default()
                }),
            )
            .await;
            if now_locked {
                self.metrics.record_auth_lockout();
                self.emit_event(
                    AuthEvent::new(
                        topics::USER_LOCKED,
                        event_user_id.clone(),
                        event_tenant.clone(),
                        serde_json::json!({
                            "user_id": event_user_id.clone(),
                            "attempt_count": attempt_count,
                            "ip_address": event_ip.clone(),
                            "locked_until_unix": locked_until_unix,
                        }),
                    )
                    .with_correlation(format!("user_locked:{event_user_id}"))
                    .with_compliance(ComplianceEnvelope {
                        actor: event_user_id.clone(),
                        target_resource: event_user_id.clone(),
                        operation: "lock".to_string(),
                        outcome: "success".to_string(),
                        reason_code: "max_failed_logins".to_string(),
                        auth_method: "password".to_string(),
                        source_ip: event_ip.clone(),
                        user_agent: event_ua.clone(),
                        ..ComplianceEnvelope::default()
                    }),
                )
                .await;
            }
            self.metrics.record_auth_login(false);
            return Err(crate::runtime::executor_utils::unauthenticated_status(
                "invalid_login",
                "invalid username or password",
            ));
        }
        // Password proven — now it is safe to surface account status.
        if user.status != crate::runtime::authn::AccountStatus::Active
            && user.status != crate::runtime::authn::AccountStatus::PendingVerification
        {
            return Err(password_login_user_active_status());
        }
        let prior_failed_login_count = user.failed_login_count;
        let prior_locked_until_unix = user.locked_until_unix;
        let prior_last_login_at_unix = user.last_login_at_unix;
        let prior_updated_at_unix = user.updated_at_unix;
        let prior_password_hash = user.password_hash.clone();
        user.failed_login_count = 0;
        user.locked_until_unix = 0;
        user.last_login_at_unix = now;
        user.updated_at_unix = now;
        // Transparently re-hash legacy keyed-HMAC passwords with Argon2id on
        // the first successful login after the KDF upgrade.
        if authn::password_hash_needs_upgrade(&user.password_hash) {
            user.password_hash = authn::hash_password(&req.password, &self.password_hash_key());
        }
        self.users.put_user(user.clone()).await.map_err(|err| {
            login_internal_status("password_login_success_update", err.to_string())
        })?;
        // Tenant MFA-enforcement policy: when the tenant requires MFA, a user who
        // has not yet enrolled a second factor cannot complete a password-only
        // login — they must enrol first.
        if !user.mfa_enabled
            && self
                .users
                .tenant_requires_mfa(&user.tenant_id)
                .await
                .map_err(|err| {
                    login_internal_status("password_login_tenant_mfa_policy", err.to_string())
                })?
        {
            return Err(tenant_mfa_enrollment_required_status());
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
                let hash = authn::hash_recovery_code(&req.recovery_code, &self.otp_hash_key()?);
                self.users
                    .consume_recovery_code(&user.user_id, &hash, now)
                    .await
                    .map_err(|err| {
                        login_internal_status(
                            "password_login_consume_recovery_code",
                            err.to_string(),
                        )
                    })?
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
                authn::totp::decrypt_secret(&user.totp_secret_hash, &self.otp_hash_key()?)
                    .map(|secret| authn::totp::verify(&secret, &req.totp_code, now))
                    .unwrap_or(false)
            };
            // A single-use recovery/backup code was consumed to satisfy MFA
            // (security-sensitive — the raw code never enters the body).
            if has_recovery && verified {
                self.emit_event(
                    AuthEvent::new(
                        topics::RECOVERY_CODE_USED,
                        user.user_id.clone(),
                        user.tenant_id.clone(),
                        serde_json::json!({
                            "user_id": user.user_id.clone(),
                            "ip_address": req.ip_address.clone(),
                        }),
                    )
                    .with_correlation(format!("recovery_code_used:{}", user.user_id))
                    .with_compliance(ComplianceEnvelope {
                        actor: user.user_id.clone(),
                        target_resource: user.user_id.clone(),
                        operation: "recovery_code_use".to_string(),
                        outcome: "success".to_string(),
                        reason_code: "recovery_code_consumed".to_string(),
                        auth_method: "recovery_code".to_string(),
                        source_ip: req.ip_address.clone(),
                        user_agent: req.user_agent.clone(),
                        ..ComplianceEnvelope::default()
                    }),
                )
                .await;
            }
            if !verified {
                self.metrics.record_auth_mfa_failure();
                self.metrics.record_auth_login(false);
                self.emit_event(
                    AuthEvent::new(
                        topics::LOGIN_FAILED,
                        user.user_id.clone(),
                        user.tenant_id.clone(),
                        serde_json::json!({
                            "user_id": user.user_id.clone(),
                            "ip_address": req.ip_address.clone(),
                            "stage": "second_factor",
                        }),
                    )
                    .with_correlation(format!("login_failed:{}", user.user_id))
                    .with_compliance(ComplianceEnvelope {
                        actor: user.user_id.clone(),
                        target_resource: user.user_id.clone(),
                        operation: "login".to_string(),
                        outcome: "failure".to_string(),
                        reason_code: "invalid_second_factor".to_string(),
                        auth_method: "mfa".to_string(),
                        source_ip: req.ip_address.clone(),
                        user_agent: req.user_agent.clone(),
                        ..ComplianceEnvelope::default()
                    }),
                )
                .await;
                return Err(crate::runtime::executor_utils::unauthenticated_status(
                    "invalid_second_factor",
                    "invalid second factor",
                ));
            }
        }
        // Block 1 (auth_fix.md, Decision E): resolve the user's role-derived
        // grants ONCE from the warm authz snapshot and thread them into both the
        // session record (the refresh carrier) and the issued JWT below.
        // fix_plan §2.2: for a service account, the TYPED durable grant (when
        // one exists) is authoritative over profile attributes.
        let typed_grant = if user.account_kind == authn::AccountKind::ServiceAccount {
            match self.pg_pool.as_ref() {
                Some(pool) => {
                    super::super::grants::get_grant_by_user(pool, &user.tenant_id, &user.user_id)
                        .await?
                }
                None => None,
            }
        } else {
            None
        };
        let grants = self.resolve_password_login_grants_with_typed(
            &user,
            &requested_scopes,
            typed_grant.as_ref(),
        )?;
        let scopes = grants.scopes.clone();
        let roles = grants.roles.clone();
        let session_result = self
            .create_login_session(
                &user,
                format!("{}|{}|{}", req.device_name, req.ip_address, req.user_agent),
                scopes.clone(),
                roles.clone(),
                grants.service_identity.clone(),
                now,
            )
            .await;
        let (session_id, _expires) = match session_result {
            Ok(session) => session,
            Err(err) => {
                let mut restored = user.clone();
                restored.failed_login_count = prior_failed_login_count;
                restored.locked_until_unix = prior_locked_until_unix;
                restored.last_login_at_unix = prior_last_login_at_unix;
                restored.updated_at_unix = prior_updated_at_unix;
                restored.password_hash = prior_password_hash;
                if let Err(restore_err) = self.users.put_user(restored).await {
                    tracing::warn!(
                        user_id = %user.user_id,
                        error = %restore_err,
                        "failed to restore login state after session creation failure"
                    );
                }
                return Err(err);
            }
        };
        self.metrics.record_auth_login(true);
        self.emit_event(
            AuthEvent::new(
                topics::USER_LOGGED_IN,
                user.user_id.clone(),
                user.tenant_id.clone(),
                serde_json::json!({
                    "user_id": user.user_id.clone(),
                    "session_public_id": public_session_handle_from_hash(&authn::hash_secret(&session_id, &self.hash_key())),
                    "tenant_id": user.tenant_id.clone(),
                    "project_id": user.project_id.clone(),
                    "device_name": req.device_name.clone(),
                    "ip_address": req.ip_address.clone(),
                }),
            )
            .with_correlation(format!("login:{}", user.user_id))
            .with_compliance(ComplianceEnvelope {
                actor: user.user_id.clone(),
                actor_project: user.project_id.clone(),
                target_resource: user.user_id.clone(),
                operation: "login".to_string(),
                outcome: "success".to_string(),
                reason_code: "credentials_verified".to_string(),
                auth_method: if user.mfa_enabled { "mfa" } else { "password" }.to_string(),
                source_ip: req.ip_address.clone(),
                user_agent: req.user_agent.clone(),
                ..ComplianceEnvelope::default()
            }),
        )
        .await;
        // Issue a short-lived signed access token when JWT signing is configured;
        // the server-side session id doubles as the refresh credential. The token
        // carries the grants resolved above so the coarse `udb:admin` gate sees
        // the admin's projected scope.
        let (access_token, access_exp) = self.issue_access_token(
            &user.user_id,
            &user.tenant_id,
            &user.project_id,
            &scopes,
            &roles,
            &grants.service_identity,
            &session_id,
            "pwd",
            super::account_kind_to_proto(user.account_kind),
            now,
        );
        let access_token_expires_in = if access_exp > 0 {
            (access_exp - now as i64).max(0) as i32
        } else {
            self.config.session_ttl_secs as i32
        };
        // Phase 3 (I2.2): the refresh credential is a token-family token
        // (rt_<family>.<jti>), NOT the server-side session id. It is rotated on
        // every RefreshToken call and reuse of a superseded value revokes the
        // family. Falls back to empty when no Postgres pool backs the registry.
        // P3 (bug_report.md): register/refresh this device so ListDevices returns
        // it and RevokeDevice is reachable. `req.device_id` is the client device
        // fingerprint (LoginRequest field 7). Best-effort; empty when no fingerprint.
        let login_device_id = self
            .register_login_device(
                &user.user_id,
                &user.tenant_id,
                &user.project_id,
                &req.device_id,
                &req.device_name,
                &req.ip_address,
            )
            .await
            .unwrap_or_default();
        let refresh_token = if user.account_kind == authn::AccountKind::ServiceAccount {
            String::new()
        } else {
            self.mint_refresh_family(
                &user.user_id,
                &user.user_id,
                &user.tenant_id,
                &user.project_id,
                &login_device_id,
                &session_id,
                now,
            )
            .await
            .unwrap_or_default()
        };
        let refresh_token_expires_in = if refresh_token.is_empty() {
            0
        } else {
            self.config.session_ttl_secs as i32
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
            refresh_token_expires_in,
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
            .map_err(|err| {
                login_invalid_fields(
                    err,
                    [(
                        "new_password",
                        "must satisfy the configured password policy",
                    )],
                )
            })?;
        let mut user = self
            .users
            .get_user_by_id(&req.user_id)
            .await
            .map_err(|err| login_internal_status("change_password_user_load", err.to_string()))?
            .ok_or_else(super::authn_user_not_found_status)?;
        if !authn::verify_password(
            &req.current_password,
            &self.password_hash_key(),
            &user.password_hash,
        ) {
            return Err(crate::runtime::executor_utils::unauthenticated_status(
                "invalid_current_password",
                "invalid current password",
            ));
        }
        if !req.otp_id.trim().is_empty() {
            let otp = self
                .users
                .get_otp(&req.otp_id)
                .await
                .map_err(|err| login_internal_status("change_password_otp_load", err.to_string()))?
                .ok_or_else(super::authn_otp_not_found_status)?;
            // The OTP must belong to the user, be consumed, AND be of a
            // sensitive-operation / password-reset type — otherwise a consumed
            // login-2FA or email-verification OTP would satisfy the gate.
            let type_ok = otp.otp_type == authn_entity_pb::OtpType::PasswordReset as i32
                || otp.otp_type == authn_entity_pb::OtpType::SensitiveOperation as i32;
            if otp.user_id != user.user_id
                || otp.status != authn_entity_pb::OtpStatus::Used as i32
                || !type_ok
            {
                return Err(password_change_otp_verified_status());
            }
        }
        let now = now_unix();
        let event_tenant = user.tenant_id.clone();
        user.password_hash = authn::hash_password(&req.new_password, &self.password_hash_key());
        user.updated_at_unix = now;
        self.users
            .put_user(user)
            .await
            .map_err(|err| login_internal_status("change_password_store", err.to_string()))?;
        self.emit_event(
            AuthEvent::new(
                topics::PASSWORD_CHANGED,
                req.user_id.clone(),
                event_tenant,
                serde_json::json!({
                    "user_id": req.user_id.clone(),
                    "is_reset": false,
                    "changed_by": req.user_id.clone(),
                }),
            )
            .with_correlation(format!("password_change:{}", req.user_id))
            .with_compliance(ComplianceEnvelope {
                actor: req.user_id.clone(),
                target_resource: req.user_id.clone(),
                operation: "password_change".to_string(),
                outcome: "success".to_string(),
                reason_code: "self_service_change".to_string(),
                auth_method: "password".to_string(),
                ..ComplianceEnvelope::default()
            }),
        )
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
            .map_err(|err| {
                login_internal_status("forgot_password_username_lookup", err.to_string())
            })? {
            Some(u) => Some(u),
            None => self
                .users
                .get_user_by_email(&identifier)
                .await
                .map_err(|err| {
                    login_internal_status("forgot_password_email_lookup", err.to_string())
                })?,
        };
        let (otp_id, code) = if let Some(user) = user {
            let (otp_id, code) = self
                .issue_otp(
                    &user,
                    authn_entity_pb::OtpType::PasswordReset as i32,
                    format!("forgot_password:{}", user.user_id),
                    now_unix(),
                )
                .await?;
            // Audit the reset request (only for a real account — emitting on a
            // miss would both leak account existence and lack a tenant/actor for
            // the compliance envelope).
            self.emit_event(
                AuthEvent::new(
                    topics::PASSWORD_RESET_REQUESTED,
                    user.user_id.clone(),
                    user.tenant_id.clone(),
                    serde_json::json!({
                        "user_id": user.user_id.clone(),
                        "otp_id": otp_id.clone(),
                    }),
                )
                .with_correlation(format!("password_reset:{}", user.user_id))
                .with_compliance(ComplianceEnvelope {
                    actor: user.user_id.clone(),
                    target_resource: user.user_id.clone(),
                    operation: "password_reset_request".to_string(),
                    outcome: "success".to_string(),
                    reason_code: "reset_otp_issued".to_string(),
                    auth_method: "otp".to_string(),
                    ..ComplianceEnvelope::default()
                }),
            )
            .await;
            (otp_id, code)
        } else {
            // Uniform (non-enumerating) miss branch: a throwaway otp_id and NO echo.
            (Uuid::new_v4().to_string(), String::new())
        };
        // Dev-only echo of the reset OTP under the single fail-closed gate (empty in
        // production / when the env opt-in is unset). The miss branch already carries
        // an empty `code`, so the response shape stays uniform.
        let dev_otp_code = if super::mfa::otp_dev_echo_enabled() {
            code
        } else {
            String::new()
        };
        Ok(Response::new(authn_pb::ForgotPasswordResponse {
            otp_id,
            dev_otp_code,
        }))
    }

    pub(super) async fn reset_password_impl(
        &self,
        request: Request<authn_pb::ResetPasswordRequest>,
    ) -> Result<Response<authn_pb::ResetPasswordResponse>, Status> {
        let req = request.into_inner();
        Self::require_uuid_arg(&req.otp_id, "otp_id")?;
        authn::PasswordPolicy::from_env()
            .validate(&req.new_password)
            .map_err(|err| {
                login_invalid_fields(
                    err,
                    [(
                        "new_password",
                        "must satisfy the configured password policy",
                    )],
                )
            })?;
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
            .ok_or_else(reset_password_request_valid_status)?;
        let mut user = self
            .users
            .get_user_by_id(&rec.user_id)
            .await
            .map_err(|err| login_internal_status("reset_password_user_load", err.to_string()))?
            .ok_or_else(super::authn_user_not_found_status)?;
        user.password_hash = authn::hash_password(&req.new_password, &self.password_hash_key());
        user.failed_login_count = 0;
        user.locked_until_unix = 0;
        user.updated_at_unix = now;
        let user_id = user.user_id.clone();
        let tenant = user.tenant_id.clone();
        self.users
            .put_user(user)
            .await
            .map_err(|err| login_internal_status("reset_password_store", err.to_string()))?;
        // Invalidate every existing session after a reset.
        let _ = self.sessions.revoke_all_for_principal(&user_id, now).await;
        self.emit_event(
            AuthEvent::new(
                topics::PASSWORD_CHANGED,
                user_id.clone(),
                tenant.clone(),
                serde_json::json!({
                    "user_id": user_id.clone(),
                    "is_reset": true,
                    "changed_by": user_id.clone(),
                }),
            )
            .with_correlation(format!("password_reset:{user_id}"))
            .with_compliance(ComplianceEnvelope {
                actor: user_id.clone(),
                target_resource: user_id.clone(),
                operation: "password_change".to_string(),
                outcome: "success".to_string(),
                reason_code: "password_reset".to_string(),
                auth_method: "otp".to_string(),
                ..ComplianceEnvelope::default()
            }),
        )
        .await;
        // Distinct reset-completed audit event (the reset flow proved control via
        // the PASSWORD_RESET OTP rather than the current password).
        self.emit_event(
            AuthEvent::new(
                topics::PASSWORD_RESET_COMPLETED,
                user_id.clone(),
                tenant,
                serde_json::json!({
                    "user_id": user_id.clone(),
                    "otp_id": req.otp_id.clone(),
                }),
            )
            .with_correlation(format!("password_reset:{user_id}"))
            .with_compliance(ComplianceEnvelope {
                actor: user_id.clone(),
                target_resource: user_id.clone(),
                operation: "password_reset_complete".to_string(),
                outcome: "success".to_string(),
                reason_code: "reset_otp_verified".to_string(),
                auth_method: "otp".to_string(),
                ..ComplianceEnvelope::default()
            }),
        )
        .await;
        Ok(Response::new(authn_pb::ResetPasswordResponse {
            user_id,
            changed_at_unix: now as i64,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::udb::core::authn::services::v1::authn_service_server::AuthnService;
    use crate::proto::{ErrorDetail, ErrorKind};
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;

    fn svc() -> AuthnServiceImpl {
        AuthnServiceImpl::new(
            authn::AuthnConfig::default(),
            crate::runtime::security::SecurityConfig::default(),
        )
    }

    fn decode_detail(status: &Status) -> ErrorDetail {
        let raw = status
            .metadata()
            .get_bin(ERROR_DETAIL_METADATA_KEY)
            .expect("typed detail trailer is present");
        crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
    }

    fn assert_validation_fields(status: &Status, expected: &[(&str, &str)]) {
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), expected.len());
        for (actual, (field, description)) in detail.field_violations.iter().zip(expected) {
            assert_eq!(actual.field, *field);
            assert_eq!(actual.description, *description);
        }
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
    fn login_internal_status_carries_typed_detail() {
        let status = login_internal_status("password_login_user_lookup", "login lookup failed");
        assert_internal_detail(&status, "password_login_user_lookup", "login lookup failed");
    }

    #[test]
    fn oidc_feature_required_status_carries_capability_detail() {
        let err = oidc_feature_required_status();

        assert_capability_detail(
            &err,
            "oidc_authentication",
            "oidc_feature",
            "OIDC authentication requires building UDB with the `oidc` feature",
        );
    }

    #[test]
    fn tenant_mfa_enrollment_policy_carries_typed_detail() {
        assert_policy_detail(
            &tenant_mfa_enrollment_required_status(),
            "password_login",
            "tenant_mfa_enrollment_required",
            "MFA enrollment required by tenant policy",
        );
    }

    #[test]
    fn login_password_policy_denials_carry_permission_detail() {
        assert_permission_policy_detail(
            &password_login_user_active_status(),
            "password_login",
            "user_not_active",
            "user is not active",
        );
        assert_permission_policy_detail(
            &password_change_otp_verified_status(),
            "change_password",
            "password_change_otp_verified",
            "password-change OTP is not verified",
        );
        assert_permission_policy_detail(
            &reset_password_request_valid_status(),
            "reset_password",
            "reset_request_valid",
            "invalid reset request",
        );
    }

    #[tokio::test]
    async fn authenticate_missing_credential_carries_field_violations() {
        let err = svc()
            .authenticate(Request::new(authn_pb::AuthnRequest::default()))
            .await
            .expect_err("missing credential must fail before any store lookup");

        assert_eq!(
            err.message(),
            "no credential supplied (set api_key, session_id, bearer_token, or external_provider_id+external_token)"
        );
        assert_validation_fields(
            &err,
            &[
                ("api_key", "must include one supported credential"),
                ("session_id", "must include one supported credential"),
                ("bearer_token", "must include one supported credential"),
                ("external_token", "must include one supported credential"),
            ],
        );
    }

    #[tokio::test]
    async fn change_password_weak_new_password_carries_field_violation() {
        let err = svc()
            .change_password(Request::new(authn_pb::ChangePasswordRequest {
                new_password: "short".to_string(),
                ..Default::default()
            }))
            .await
            .expect_err("weak new password must fail before user lookup");

        assert_eq!(err.message(), "password must be at least 10 characters");
        assert_validation_fields(
            &err,
            &[(
                "new_password",
                "must satisfy the configured password policy",
            )],
        );
    }

    #[tokio::test]
    async fn reset_password_weak_new_password_carries_field_violation() {
        let err = svc()
            .reset_password(Request::new(authn_pb::ResetPasswordRequest {
                otp_id: "00000000-0000-0000-0000-000000000001".to_string(),
                new_password: "short".to_string(),
                ..Default::default()
            }))
            .await
            .expect_err("weak new password must fail before otp lookup");

        assert_eq!(err.message(), "password must be at least 10 characters");
        assert_validation_fields(
            &err,
            &[(
                "new_password",
                "must satisfy the configured password policy",
            )],
        );
    }
}
