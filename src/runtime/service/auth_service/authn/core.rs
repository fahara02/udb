//! User lifecycle: create / read / list / update, status changes, and
//! admin-initiated password reset.

use super::*;

pub(super) fn user_record_to_pb(rec: &UserRecord) -> authn_entity_pb::User {
    authn_entity_pb::User {
        user_id: rec.user_id.clone(),
        username: rec.username.clone(),
        email: rec.email.clone(),
        password_hash: rec.password_hash.clone(),
        account_kind: rec.account_kind,
        status: rec.status,
        tenant_id: rec.tenant_id.clone(),
        full_name: rec.full_name.clone(),
        totp_secret_enc: rec.totp_secret_hash.clone(),
        mfa_enabled: rec.mfa_enabled,
        failed_login_count: rec.failed_login_count,
        locked_until: timestamp_from_unix(rec.locked_until_unix),
        email_verified_at: timestamp_from_unix(rec.email_verified_at_unix),
        last_login_at: timestamp_from_unix(rec.last_login_at_unix),
        created_by: rec.created_by.clone(),
        created_at: timestamp_from_unix(rec.created_at_unix),
        updated_at: timestamp_from_unix(rec.updated_at_unix),
        deleted_at: timestamp_from_unix(rec.deleted_at_unix),
        deleted_by: rec.deleted_by.clone(),
        project_id: rec.project_id.clone(),
        external_provider_id: rec.external_provider_id.clone(),
        external_subject: rec.external_subject.clone(),
        locale: String::new(),
        timezone: String::new(),
        profile_attributes_json: rec.profile_attributes_json.clone(),
        external_references_json: "[]".to_string(),
        // Phone is persisted/verified via dedicated UPDATEs (set_user_phone /
        // mark_phone_verified), not threaded through UserRecord; surfacing it in
        // GetUser is a follow-up.
        phone: String::new(),
        phone_verified_at: None,
    }
}

impl AuthnServiceImpl {
    pub(super) async fn create_user_impl(
        &self,
        request: Request<authn_pb::CreateUserRequest>,
    ) -> Result<Response<authn_pb::CreateUserResponse>, Status> {
        if self.password_hash_key().is_empty() {
            return Err(Status::failed_precondition(
                "native user passwords require UDB_PASSWORD_HASH_SECRET or UDB_SESSION_HASH_SECRET",
            ));
        }
        let req = request.into_inner();
        if req.username.trim().is_empty() || req.email.trim().is_empty() {
            return Err(Status::invalid_argument("username and email are required"));
        }
        // Externally-provisioned (SSO/OIDC) users have no local password to vet.
        if req.external_provider_id.trim().is_empty() {
            authn::PasswordPolicy::from_env()
                .validate(&req.password)
                .map_err(Status::invalid_argument)?;
        }
        let now = now_unix();
        let user_id = Uuid::new_v4().to_string();
        let account_kind = if req.account_kind == authn_entity_pb::AccountKind::Unspecified as i32 {
            authn_entity_pb::AccountKind::Person as i32
        } else {
            req.account_kind
        };
        let created_by = req
            .context
            .as_ref()
            .map(|ctx| ctx.principal_id.clone())
            .unwrap_or_default();
        let rec = UserRecord {
            user_id: user_id.clone(),
            username: req.username.trim().to_ascii_lowercase(),
            email: req.email.trim().to_ascii_lowercase(),
            password_hash: authn::hash_password(&req.password, &self.password_hash_key()),
            account_kind,
            status: authn_entity_pb::UserStatus::PendingVerification as i32,
            tenant_id: req.tenant_id,
            full_name: req.full_name,
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
            project_id: req.project_id,
            external_provider_id: req.external_provider_id,
            external_subject: req.external_subject,
            profile_attributes_json: serde_json::to_string(&req.profile_attributes)
                .unwrap_or_else(|_| "{}".to_string()),
        };
        self.users
            .put_user(rec.clone())
            .await
            .map_err(Status::internal)?;
        let (otp_id, _code) = self
            .issue_otp(
                &rec,
                authn_entity_pb::OtpType::EmailVerification as i32,
                format!("create_user:{user_id}"),
                now,
            )
            .await?;
        self.emit_event(
            AuthEvent::new(
                topics::USER_REGISTERED,
                rec.user_id.clone(),
                rec.tenant_id.clone(),
                serde_json::json!({
                    "user_id": rec.user_id.clone(),
                    "username": rec.username.clone(),
                    "email": rec.email.clone(),
                    "tenant_id": rec.tenant_id.clone(),
                    "project_id": rec.project_id.clone(),
                    "account_kind": rec.account_kind,
                    "created_by": rec.created_by.clone(),
                }),
            )
            .with_correlation(format!("create_user:{user_id}")),
        )
        .await;
        Ok(Response::new(authn_pb::CreateUserResponse {
            user: Some(user_record_to_pb(&rec)),
            otp_id,
        }))
    }

    pub(super) async fn get_user_impl(
        &self,
        request: Request<authn_pb::GetUserRequest>,
    ) -> Result<Response<authn_pb::GetUserResponse>, Status> {
        let req = request.into_inner();
        let user = if !req.user_id.trim().is_empty() {
            self.users.get_user_by_id(&req.user_id).await
        } else if !req.username.trim().is_empty() {
            self.users
                .get_user_by_username(&req.username.to_ascii_lowercase())
                .await
        } else if !req.email.trim().is_empty() {
            self.users
                .get_user_by_email(&req.email.to_ascii_lowercase())
                .await
        } else {
            return Err(Status::invalid_argument(
                "one of user_id, username, or email is required",
            ));
        }
        .map_err(Status::internal)?
        .ok_or_else(|| Status::not_found("user not found"))?;
        Ok(Response::new(authn_pb::GetUserResponse {
            user: Some(user_record_to_pb(&user)),
        }))
    }

    pub(super) async fn list_users_impl(
        &self,
        request: Request<authn_pb::ListUsersRequest>,
    ) -> Result<Response<authn_pb::ListUsersResponse>, Status> {
        let req = request.into_inner();
        let page = req.page.as_ref();
        let (limit, offset, _) = bounded_page_window(page);
        let (users, total) = self
            .users
            .list_users_page(&req.tenant_id, req.account_kind, req.status, limit, offset)
            .await
            .map_err(Status::internal)?;
        let users = users.iter().map(user_record_to_pb).collect();
        Ok(Response::new(authn_pb::ListUsersResponse {
            users,
            page: Some(bounded_page_response(total, page)),
        }))
    }

    pub(super) async fn update_user_impl(
        &self,
        request: Request<authn_pb::UpdateUserRequest>,
    ) -> Result<Response<authn_pb::UpdateUserResponse>, Status> {
        let req = request.into_inner();
        let mut rec = self
            .users
            .get_user_by_id(&req.user_id)
            .await
            .map_err(Status::internal)?
            .ok_or_else(|| Status::not_found("user not found"))?;
        if !req.full_name.trim().is_empty() {
            rec.full_name = req.full_name;
        }
        if !req.email.trim().is_empty() {
            rec.email = req.email.trim().to_ascii_lowercase();
        }
        if !req.tenant_id.trim().is_empty() {
            rec.tenant_id = req.tenant_id;
        }
        if req.account_kind != authn_entity_pb::AccountKind::Unspecified as i32 {
            rec.account_kind = req.account_kind;
        }
        if !req.project_id.trim().is_empty() {
            rec.project_id = req.project_id;
        }
        if !req.external_provider_id.trim().is_empty() {
            rec.external_provider_id = req.external_provider_id;
        }
        if !req.external_subject.trim().is_empty() {
            rec.external_subject = req.external_subject;
        }
        if !req.profile_attributes.is_empty() {
            rec.profile_attributes_json =
                serde_json::to_string(&req.profile_attributes).unwrap_or_else(|_| "{}".to_string());
        }
        rec.updated_at_unix = now_unix();
        self.users
            .put_user(rec.clone())
            .await
            .map_err(Status::internal)?;
        Ok(Response::new(authn_pb::UpdateUserResponse {
            user: Some(user_record_to_pb(&rec)),
        }))
    }

    pub(super) async fn change_user_status_impl(
        &self,
        request: Request<authn_pb::ChangeUserStatusRequest>,
    ) -> Result<Response<authn_pb::ChangeUserStatusResponse>, Status> {
        let req = request.into_inner();
        if req.new_status == authn_entity_pb::UserStatus::Unspecified as i32 {
            return Err(Status::invalid_argument("new_status is required"));
        }
        let mut rec = self
            .users
            .get_user_by_id(&req.user_id)
            .await
            .map_err(Status::internal)?
            .ok_or_else(|| Status::not_found("user not found"))?;
        let old_status = rec.status;
        rec.status = req.new_status;
        rec.updated_at_unix = now_unix();
        self.users
            .put_user(rec.clone())
            .await
            .map_err(Status::internal)?;
        self.emit_event(AuthEvent::new(
            topics::USER_STATUS_CHANGED,
            rec.user_id.clone(),
            rec.tenant_id.clone(),
            serde_json::json!({
                "user_id": rec.user_id.clone(),
                "old_status": old_status,
                "new_status": req.new_status,
                "reason": req.reason.clone(),
                "tenant_id": rec.tenant_id.clone(),
            }),
        ))
        .await;
        Ok(Response::new(authn_pb::ChangeUserStatusResponse {
            user: Some(user_record_to_pb(&rec)),
        }))
    }

    pub(super) async fn admin_reset_password_impl(
        &self,
        request: Request<authn_pb::AdminResetPasswordRequest>,
    ) -> Result<Response<authn_pb::AdminResetPasswordResponse>, Status> {
        let req = request.into_inner();
        let user = self
            .users
            .get_user_by_id(&req.user_id)
            .await
            .map_err(Status::internal)?
            .ok_or_else(|| Status::not_found("user not found"))?;
        let (otp_id, _code) = self
            .issue_otp(
                &user,
                authn_entity_pb::OtpType::PasswordReset as i32,
                format!("admin_reset_password:{}", user.user_id),
                now_unix(),
            )
            .await?;
        Ok(Response::new(authn_pb::AdminResetPasswordResponse { otp_id }))
    }
}
