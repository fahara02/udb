//! Server-side sessions: creation/refresh/revocation, listing, logout, CSRF
//! validation, and token (session / api-key / JWT) validation.

use super::*;

fn validate_session_response(
    rec: Option<SessionRecord>,
    raw_session_id: &str,
    now_unix: u64,
) -> authn_pb::ValidateTokenResponse {
    let Some(rec) = rec else {
        return authn_pb::ValidateTokenResponse {
            valid: false,
            ..Default::default()
        };
    };
    let principal = principal_from_session(&rec);
    authn_pb::ValidateTokenResponse {
        valid: true,
        user_id: rec.user_id.clone(),
        session_id: raw_session_id.to_string(),
        account_kind: authn_entity_pb::AccountKind::Unspecified as i32,
        tenant_id: rec.tenant_id.clone(),
        roles: rec.roles.clone(),
        expires_at: timestamp_from_unix(rec.expires_at_unix),
        access_surface: "session".to_string(),
        device_id: rec.client_fingerprint.clone(),
        token_id: rec.session_id_hash.chars().take(24).collect(),
        session_type: authn_entity_pb::SessionType::ServerSide as i32,
        principal: Some(authn_principal_to_pb(
            &principal,
            rec.expires_at_unix as i64,
        )),
        project_id: rec.project_id.clone(),
        scopes: rec.scopes.clone(),
        attributes: [
            ("active".to_string(), rec.is_active(now_unix).to_string()),
            (
                "relationship_version".to_string(),
                rec.relationship_version.clone(),
            ),
        ]
        .into_iter()
        .collect(),
    }
}

fn validate_api_key_response(rec: Option<authn::ApiKeyRecord>) -> authn_pb::ValidateTokenResponse {
    let Some(rec) = rec else {
        return authn_pb::ValidateTokenResponse {
            valid: false,
            ..Default::default()
        };
    };
    let principal = principal_from_api_key(&rec);
    authn_pb::ValidateTokenResponse {
        valid: true,
        user_id: String::new(),
        session_id: String::new(),
        account_kind: authn_entity_pb::AccountKind::ServiceAccount as i32,
        tenant_id: rec.tenant_id.clone(),
        roles: Vec::new(),
        expires_at: timestamp_from_unix(rec.expires_at_unix),
        access_surface: "api_key".to_string(),
        device_id: String::new(),
        token_id: rec.key_prefix.clone(),
        session_type: authn_entity_pb::SessionType::ApiKey as i32,
        principal: Some(authn_principal_to_pb(
            &principal,
            rec.expires_at_unix as i64,
        )),
        project_id: rec.project_id.clone(),
        scopes: rec.scopes.clone(),
        attributes: Default::default(),
    }
}

impl AuthnServiceImpl {
    /// Create a server-side login session for `user`, returning the raw session
    /// id (the refresh credential) and its absolute expiry.
    pub(super) async fn create_login_session(
        &self,
        user: &UserRecord,
        client_fingerprint: String,
        now: u64,
    ) -> Result<(String, u64), Status> {
        if !self.config.sessions_usable() {
            return Err(Status::failed_precondition(
                "sessions disabled (set UDB_SESSION_ENABLED and UDB_SESSION_HASH_SECRET)",
            ));
        }
        let raw_session_id = format!("sess_{}", Uuid::new_v4().simple());
        let expires = now.saturating_add(self.config.session_ttl_secs);
        let rec = SessionRecord {
            session_id_hash: authn::hash_secret(&raw_session_id, &self.hash_key()),
            principal_id: user.user_id.clone(),
            user_id: user.user_id.clone(),
            service_identity: String::new(),
            tenant_id: user.tenant_id.clone(),
            project_id: user.project_id.clone(),
            scopes: Vec::new(),
            roles: Vec::new(),
            relationship_version: String::new(),
            created_at_unix: now,
            updated_at_unix: now,
            expires_at_unix: expires,
            revoked_at_unix: 0,
            client_fingerprint,
        };
        self.sessions.put(&rec).await.map_err(Status::internal)?;
        Ok((raw_session_id, expires))
    }

    pub(super) async fn create_session_impl(
        &self,
        request: Request<authn_pb::CreateSessionRequest>,
    ) -> Result<Response<authn_pb::CreateSessionResponse>, Status> {
        if !self.config.sessions_usable() {
            return Err(Status::failed_precondition(
                "sessions disabled (set UDB_SESSION_ENABLED and UDB_SESSION_HASH_SECRET)",
            ));
        }
        let req = request.into_inner();
        let p = req
            .principal
            .ok_or_else(|| Status::invalid_argument("principal is required"))?;
        let now = now_unix();
        let ttl = if req.ttl_seconds > 0 {
            req.ttl_seconds as u64
        } else {
            self.config.session_ttl_secs
        };
        let expires = now.saturating_add(ttl);
        let raw_session_id = format!("sess_{}", Uuid::new_v4().simple());
        let rec = SessionRecord {
            session_id_hash: authn::hash_secret(&raw_session_id, &self.hash_key()),
            principal_id: p.principal_id,
            user_id: p.user_id,
            service_identity: p.service_identity,
            tenant_id: p.tenant_id,
            project_id: p.project_id,
            scopes: p.scopes,
            roles: p.roles,
            relationship_version: String::new(),
            created_at_unix: now,
            updated_at_unix: now,
            expires_at_unix: expires,
            revoked_at_unix: 0,
            client_fingerprint: req.client_fingerprint,
        };
        self.sessions.put(&rec).await.map_err(Status::internal)?;
        Ok(Response::new(authn_pb::CreateSessionResponse {
            session_id: raw_session_id,
            expires_at_unix: expires as i64,
        }))
    }

    pub(super) async fn refresh_session_impl(
        &self,
        request: Request<authn_pb::RefreshSessionRequest>,
    ) -> Result<Response<authn_pb::RefreshSessionResponse>, Status> {
        let req = request.into_inner();
        let now = now_unix();
        let ttl = if req.ttl_seconds > 0 {
            req.ttl_seconds as u64
        } else {
            self.config.session_ttl_secs
        };
        match authn::refresh_session(
            self.sessions.as_ref(),
            &req.session_id,
            &self.hash_key(),
            now,
            ttl,
        )
        .await
        .map_err(Status::internal)?
        {
            Some(rec) => Ok(Response::new(authn_pb::RefreshSessionResponse {
                expires_at_unix: rec.expires_at_unix as i64,
                active: true,
            })),
            None => Ok(Response::new(authn_pb::RefreshSessionResponse {
                expires_at_unix: 0,
                active: false,
            })),
        }
    }

    pub(super) async fn revoke_session_impl(
        &self,
        request: Request<authn_pb::RevokeSessionRequest>,
    ) -> Result<Response<authn_pb::RevokeSessionResponse>, Status> {
        let req = request.into_inner();
        let now = now_unix();
        if req.all_for_principal && !req.principal_id.trim().is_empty() {
            let n = self
                .sessions
                .revoke_all_for_principal(&req.principal_id, now)
                .await
                .map_err(Status::internal)?;
            return Ok(Response::new(authn_pb::RevokeSessionResponse {
                session_id: String::new(),
                revoked_at: None,
                operation_id: Uuid::new_v4().to_string(),
                revoked_count: n as i32,
            }));
        }
        let hash = authn::hash_secret(&req.session_id, &self.hash_key());
        let ok = self
            .sessions
            .revoke(&hash, now)
            .await
            .map_err(Status::internal)?;
        if ok {
            self.emit_event(AuthEvent::new(
                topics::SESSION_REVOKED,
                req.session_id.clone(),
                String::new(),
                serde_json::json!({
                    "session_id": req.session_id.clone(),
                    "revoke_reason": req.revoke_reason.clone(),
                    "revoked_by": req.principal_id.clone(),
                }),
            ))
            .await;
        }
        Ok(Response::new(authn_pb::RevokeSessionResponse {
            session_id: req.session_id,
            revoked_at: None,
            operation_id: Uuid::new_v4().to_string(),
            revoked_count: i32::from(ok),
        }))
    }

    pub(super) async fn refresh_token_impl(
        &self,
        request: Request<authn_pb::RefreshTokenRequest>,
    ) -> Result<Response<authn_pb::RefreshTokenResponse>, Status> {
        let req = request.into_inner();
        let now = now_unix();
        // The refresh credential is a server-side session id (returned as
        // `refresh_token` at login). Accept it from either field for
        // compatibility with session-only callers.
        let session_ref = if !req.refresh_token.trim().is_empty() {
            req.refresh_token.clone()
        } else {
            req.session_id.clone()
        };
        if session_ref.trim().is_empty() {
            return Err(Status::invalid_argument(
                "refresh_token or session_id is required",
            ));
        }
        // Sliding refresh: extend the backing session, then mint a fresh
        // short-lived access token bound to it.
        let Some(rec) = authn::refresh_session(
            self.sessions.as_ref(),
            &session_ref,
            &self.hash_key(),
            now,
            self.config.session_ttl_secs,
        )
        .await
        .map_err(Status::internal)?
        else {
            return Err(Status::unauthenticated("invalid or expired session"));
        };
        let (access_token, access_exp) = self.issue_access_token(
            &rec.user_id,
            &rec.tenant_id,
            &rec.project_id,
            &rec.scopes,
            &rec.roles,
            &rec.service_identity,
            &session_ref,
            now,
        );
        let access_token_expires_in = if access_exp > 0 {
            (access_exp - now as i64).max(0) as i32
        } else {
            self.config.session_ttl_secs as i32
        };
        Ok(Response::new(authn_pb::RefreshTokenResponse {
            access_token,
            access_token_expires_in,
        }))
    }

    pub(super) async fn logout_impl(
        &self,
        request: Request<authn_pb::LogoutRequest>,
    ) -> Result<Response<authn_pb::LogoutResponse>, Status> {
        let req = request.into_inner();
        let now = now_unix();
        let count = if req.all_sessions {
            let principal_id = req
                .context
                .as_ref()
                .map(|ctx| ctx.principal_id.clone())
                .unwrap_or_default();
            if principal_id.trim().is_empty() {
                return Err(Status::invalid_argument(
                    "context.principal_id is required for all_sessions logout",
                ));
            }
            self.sessions
                .revoke_all_for_principal(&principal_id, now)
                .await
                .map_err(Status::internal)? as i32
        } else {
            let hash = authn::hash_secret(&req.session_id, &self.hash_key());
            i32::from(
                self.sessions
                    .revoke(&hash, now)
                    .await
                    .map_err(Status::internal)?,
            )
        };
        Ok(Response::new(authn_pb::LogoutResponse {
            sessions_revoked: count,
        }))
    }

    pub(super) async fn validate_token_impl(
        &self,
        request: Request<authn_pb::ValidateTokenRequest>,
    ) -> Result<Response<authn_pb::ValidateTokenResponse>, Status> {
        let req = request.into_inner();
        let now = now_unix();
        let token_type = authn_entity_pb::TokenType::try_from(req.token_type).unwrap_or_default();
        let response = match token_type {
            authn_entity_pb::TokenType::Session => {
                let rec = authn::validate_session(
                    self.sessions.as_ref(),
                    &req.token,
                    &self.hash_key(),
                    now,
                    self.config.session_idle_ttl_secs,
                )
                .await
                .map_err(Status::internal)?;
                validate_session_response(rec, &req.token, now)
            }
            authn_entity_pb::TokenType::ApiKey => {
                let rec = authn::validate_api_key(
                    self.api_keys.as_ref(),
                    &req.token,
                    &self.api_key_hash_key(),
                    now,
                )
                .await
                .map_err(Status::internal)?;
                validate_api_key_response(rec)
            }
            authn_entity_pb::TokenType::JwtAccess | authn_entity_pb::TokenType::JwtRefresh => {
                let claims = validate_bearer_token(&self.security, &req.token)
                    .map_err(Status::unauthenticated)?;
                let subject = claims.sub.clone().unwrap_or_default();
                let principal = Principal {
                    principal_id: subject.clone(),
                    subject: subject.clone(),
                    user_id: subject.clone(),
                    service_identity: claims.service_identity.clone().unwrap_or_default(),
                    tenant_id: claims.tenant_id.clone().unwrap_or_default(),
                    project_id: claims.project_id.clone().unwrap_or_default(),
                    scopes: claims.resolved_scopes(),
                    roles: claims.roles.clone().unwrap_or_default(),
                    provider_id: String::new(),
                    auth_method: authn::AuthnMethod::Jwt.as_str().to_string(),
                };
                authn_pb::ValidateTokenResponse {
                    valid: true,
                    user_id: subject,
                    session_id: String::new(),
                    account_kind: authn_entity_pb::AccountKind::Unspecified as i32,
                    tenant_id: principal.tenant_id.clone(),
                    roles: principal.roles.clone(),
                    expires_at: None,
                    access_surface: "jwt".to_string(),
                    device_id: String::new(),
                    token_id: claims.jti.clone().unwrap_or_default(),
                    session_type: authn_entity_pb::SessionType::Jwt as i32,
                    principal: Some(authn_principal_to_pb(&principal, 0)),
                    project_id: principal.project_id.clone(),
                    scopes: principal.scopes.clone(),
                    attributes: Default::default(),
                }
            }
            _ => {
                return Err(Status::invalid_argument(
                    "supported token_type values are SESSION, API_KEY, JWT_ACCESS, and JWT_REFRESH",
                ));
            }
        };
        Ok(Response::new(response))
    }

    pub(super) async fn get_session_impl(
        &self,
        request: Request<authn_pb::GetSessionRequest>,
    ) -> Result<Response<authn_pb::GetSessionResponse>, Status> {
        let req = request.into_inner();
        let hash = authn::hash_secret(&req.session_id, &self.hash_key());
        let now = now_unix();
        let session = self
            .sessions
            .get(&hash)
            .await
            .map_err(Status::internal)?
            .map(|rec| session_record_to_pb(&rec, now));
        Ok(Response::new(authn_pb::GetSessionResponse { session }))
    }

    pub(super) async fn list_sessions_impl(
        &self,
        request: Request<authn_pb::ListSessionsRequest>,
    ) -> Result<Response<authn_pb::ListSessionsResponse>, Status> {
        let req = request.into_inner();
        if req.user_id.trim().is_empty() {
            return Err(Status::invalid_argument("user_id is required"));
        }
        let now = now_unix();
        let page = req.page.as_ref();
        let (limit, offset, _) = bounded_page_window(page);
        let (sessions, total) = self
            .sessions
            .list_for_principal_page(&req.user_id, req.active_only, now, limit, offset)
            .await
            .map_err(Status::internal)?;
        let sessions = sessions
            .iter()
            .map(|rec| session_record_to_pb(rec, now))
            .collect();
        Ok(Response::new(authn_pb::ListSessionsResponse {
            sessions,
            page: Some(bounded_page_response(total, page)),
        }))
    }

    pub(super) async fn validate_csrf_impl(
        &self,
        request: Request<authn_pb::ValidateCsrfRequest>,
    ) -> Result<Response<authn_pb::ValidateCsrfResponse>, Status> {
        let req = request.into_inner();
        if req.session_id.trim().is_empty() || req.csrf_token.trim().is_empty() {
            return Ok(Response::new(authn_pb::ValidateCsrfResponse {
                valid: false,
            }));
        }
        let now = now_unix();
        // Signed double-submit cookie pattern: the CSRF token is a keyed HMAC
        // bound to the session id (issued at login). Validate the session is
        // live, then constant-time compare the presented token to the expected
        // binding — a forged token can't be produced without the server secret.
        let session_live = authn::validate_session(
            self.sessions.as_ref(),
            &req.session_id,
            &self.hash_key(),
            now,
            self.config.session_idle_ttl_secs,
        )
        .await
        .map_err(Status::internal)?
        .is_some();
        let expected = self.csrf_token_for(&req.session_id);
        let token_ok = authn::constant_time_eq(&expected, &req.csrf_token);
        Ok(Response::new(authn_pb::ValidateCsrfResponse {
            valid: session_live && token_ok,
        }))
    }
}
