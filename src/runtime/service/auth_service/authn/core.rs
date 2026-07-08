//! User lifecycle: create / read / list / update, status changes, and
//! admin-initiated password reset.

use super::*;

// gRPC paths for the admin-mutation RPCs, used to look up each method's
// `endpoint_security.decision_resource`/`policy_ref` and synthesize the per-action
// authz resource (`native.rpc:<path>`) for the native authz DECISION ENGINE
// (Tier-0 #5, D2-full). Defined once here so the resource string is consistent
// with the descriptor registry key (`/<package>.<Service>/<Method>`).
const AUTHN_CREATE_USER_PATH: &str = "/udb.core.authn.services.v1.AuthnService/CreateUser";
const AUTHN_UPDATE_USER_PATH: &str = "/udb.core.authn.services.v1.AuthnService/UpdateUser";
const AUTHN_CHANGE_USER_STATUS_PATH: &str =
    "/udb.core.authn.services.v1.AuthnService/ChangeUserStatus";
const AUTHN_ADMIN_RESET_PASSWORD_PATH: &str =
    "/udb.core.authn.services.v1.AuthnService/AdminResetPassword";

fn authn_core_invalid_fields<I, F, D>(message: impl Into<String>, fields: I) -> Status
where
    I: IntoIterator<Item = (F, D)>,
    F: Into<String>,
    D: Into<String>,
{
    crate::runtime::executor_utils::invalid_argument_fields(message, fields)
}

fn create_user_password_policy_status(message: impl Into<String>) -> Status {
    authn_core_invalid_fields(
        message,
        [(
            "password",
            "must satisfy the configured password policy for native users",
        )],
    )
}

fn create_user_password_secret_status() -> Status {
    authn_capability_status(
        "native_user_passwords",
        "password_hash_secret",
        "native user passwords require UDB_PASSWORD_HASH_SECRET or UDB_SESSION_HASH_SECRET",
    )
}

fn authn_core_policy_status(
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

fn authn_core_internal_status(operation: impl Into<String>, message: impl Into<String>) -> Status {
    crate::runtime::executor_utils::internal_status("authn", operation, message)
}

fn authn_read_tenant_scope_required_status() -> Status {
    authn_core_policy_status(
        "authn_user_read",
        "tenant_scoped_bearer_required",
        "operation requires a tenant-scoped bearer token or a cross-tenant admin role",
    )
}

fn authn_read_tenant_mismatch_status() -> Status {
    authn_core_policy_status(
        "authn_user_read",
        "tenant_mismatch",
        "request tenant must match the bearer token tenant",
    )
}

fn created_by_principal_mismatch_status() -> Status {
    authn_core_policy_status(
        "create_user",
        "created_by_principal_mismatch",
        "created_by principal must match the authenticated bearer subject",
    )
}

pub(super) fn user_record_to_pb(rec: &UserRecord) -> authn_entity_pb::User {
    let mut dto = authn_entity_pb::User {
        user_id: rec.user_id.clone(),
        username: rec.username.clone(),
        email: rec.email.clone(),
        // Phase 0 (seal sensitive surfaces): credential material must never leave
        // the process through an outbound DTO. `user_record_to_pb` feeds every
        // User response (Create/Get/List/Update/ChangeStatus), so the password
        // hash and the encrypted TOTP secret are redacted here unconditionally —
        // mirroring the API-key mapper, which already blanks `key_hash`. There is
        // no legitimate caller (admin included) that needs the hash over the wire.
        password_hash: String::new(),
        account_kind: account_kind_to_proto(rec.account_kind),
        status: account_status_to_proto(rec.status),
        tenant_id: rec.tenant_id.clone(),
        full_name: rec.full_name.clone(),
        totp_secret_enc: String::new(),
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
        profile_attributes_json: crate::runtime::authn::profile::redact_profile_attributes_json(
            &rec.profile_attributes_json,
        ),
        external_references_json: "[]".to_string(),
        // Phone is persisted/verified via dedicated UPDATEs (set_user_phone /
        // mark_phone_verified), not threaded through UserRecord; surfacing it in
        // GetUser is a follow-up.
        phone: String::new(),
        phone_verified_at: None,
    };
    // §7: structurally blank every OUTPUT_VIEW_STORAGE_ONLY field (descriptor-driven
    // codegen) — covers new sensitive fields automatically, not just the hand-listed ones.
    crate::proto_redaction::RedactStorageOnly::redact_storage_only(&mut dto);
    dto
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::udb::core::authn::services::v1::authn_service_server::AuthnService;
    use crate::proto::{ErrorDetail, ErrorKind};
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;
    use prost::Message as _;
    use std::collections::BTreeSet;

    fn storage_only_field_names(message_full_name: &str) -> BTreeSet<String> {
        const OUTPUT_VIEW_STORAGE_ONLY: i32 = 1;
        let manifest = crate::runtime::descriptor_manifest::descriptor_contract_manifest_static();
        let message = manifest
            .messages
            .iter()
            .find(|message| message.full_name == message_full_name)
            .unwrap_or_else(|| panic!("descriptor message {message_full_name} must exist"));

        message
            .fields
            .iter()
            .filter(|field| {
                field
                    .db_column_security
                    .as_ref()
                    .is_some_and(|security| security.output_view == OUTPUT_VIEW_STORAGE_ONLY)
            })
            .map(|field| field.name.clone())
            .collect()
    }

    fn user_pb_string_field<'a>(pb: &'a authn_entity_pb::User, field: &str) -> &'a str {
        match field {
            "password_hash" => &pb.password_hash,
            "totp_secret_enc" => &pb.totp_secret_enc,
            other => panic!("User mapper has no storage-only assertion for {other}"),
        }
    }

    fn user_record() -> UserRecord {
        UserRecord {
            user_id: "user-1".to_string(),
            username: "ada".to_string(),
            email: "ada@example.com".to_string(),
            password_hash: "argon2id$secret".to_string(),
            account_kind: crate::runtime::authn::AccountKind::Person,
            status: crate::runtime::authn::AccountStatus::Active,
            tenant_id: "acme".to_string(),
            full_name: "Ada".to_string(),
            totp_secret_hash: "ciphertext".to_string(),
            mfa_enabled: true,
            failed_login_count: 0,
            locked_until_unix: 0,
            email_verified_at_unix: 0,
            last_login_at_unix: 0,
            created_by: "admin".to_string(),
            created_at_unix: 1,
            updated_at_unix: 2,
            deleted_at_unix: 0,
            deleted_by: String::new(),
            project_id: "billing".to_string(),
            external_provider_id: String::new(),
            external_subject: String::new(),
            profile_attributes_json: serde_json::json!({
                "display_theme": "dark",
                "_internal_score": 42,
                "nested": {
                    "api_token": "secret",
                    "nickname": "ada"
                }
            })
            .to_string(),
        }
    }

    #[test]
    fn user_mapper_redacts_storage_credentials_and_sensitive_profile_fields() {
        let pb = user_record_to_pb(&user_record());
        let attrs: serde_json::Value = serde_json::from_str(&pb.profile_attributes_json).unwrap();

        assert!(pb.password_hash.is_empty());
        assert!(pb.totp_secret_enc.is_empty());
        assert_eq!(attrs["display_theme"], "dark");
        assert_eq!(attrs["_internal_score"], "[REDACTED]");
        assert_eq!(attrs["nested"]["api_token"], "[REDACTED]");
        assert_eq!(attrs["nested"]["nickname"], "ada");
    }

    fn deny_test_service() -> AuthnServiceImpl {
        // A no-pool service is enough: the D1/D2 admin-mutation guards fire BEFORE
        // any DB access, so a cross-tenant call is denied without a live Postgres.
        // `session_hash_secret` is set so `password_hash_key()` is non-empty and the
        // create_user precondition does not short-circuit before the guard.
        let config = crate::runtime::authn::AuthnConfig {
            session_hash_secret: "deny-test-secret".to_string(),
            ..crate::runtime::authn::AuthnConfig::default()
        };
        AuthnServiceImpl::new(config, crate::runtime::security::SecurityConfig::default())
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
        assert_eq!(status.code(), tonic::Code::PermissionDenied);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Policy as i32);
        assert_eq!(detail.operation, operation);
        assert_eq!(detail.policy_decision_id, policy_decision_id);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
        assert!(detail.field_violations.is_empty());
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
    fn authn_core_internal_status_carries_typed_detail() {
        let status = authn_core_internal_status("list_users", "list users failed: store closed");
        assert_internal_detail(&status, "list_users", "list users failed: store closed");
    }

    #[test]
    fn create_user_password_secret_status_carries_capability_detail() {
        let err = create_user_password_secret_status();

        assert_capability_detail(
            &err,
            "native_user_passwords",
            "password_hash_secret",
            "native user passwords require UDB_PASSWORD_HASH_SECRET or UDB_SESSION_HASH_SECRET",
        );
    }

    #[tokio::test]
    async fn create_user_missing_identity_carries_field_violations() {
        let err = deny_test_service()
            .create_user(Request::new(authn_pb::CreateUserRequest::default()))
            .await
            .expect_err("missing username/email must fail before authz or pool access");

        assert_eq!(err.message(), "username and email are required");
        assert_validation_fields(
            &err,
            &[
                ("username", "must be a non-empty username"),
                ("email", "must be a non-empty email address"),
            ],
        );
    }

    #[tokio::test]
    async fn get_user_missing_lookup_identity_carries_field_violations() {
        let err = deny_test_service()
            .get_user(Request::new(authn_pb::GetUserRequest::default()))
            .await
            .expect_err("missing lookup identity must fail before claim/store access");

        assert_eq!(
            err.message(),
            "one of user_id, username, or email is required"
        );
        assert_validation_fields(
            &err,
            &[
                ("user_id", "must include at least one lookup identity"),
                ("username", "must include at least one lookup identity"),
                ("email", "must include at least one lookup identity"),
            ],
        );
    }

    #[tokio::test]
    async fn change_user_status_missing_new_status_carries_field_violation() {
        let err = deny_test_service()
            .change_user_status(Request::new(authn_pb::ChangeUserStatusRequest::default()))
            .await
            .expect_err("missing new_status must fail before authz or store access");

        assert_eq!(err.message(), "new_status is required");
        assert_validation_fields(&err, &[("new_status", "must specify a target user status")]);
    }

    #[test]
    fn create_user_password_policy_status_carries_field_violation() {
        let err = create_user_password_policy_status("password must contain a symbol");

        assert_eq!(err.message(), "password must contain a symbol");
        assert_validation_fields(
            &err,
            &[(
                "password",
                "must satisfy the configured password policy for native users",
            )],
        );
    }

    /// Build a deny-test service whose admin-mutation handlers decide against a
    /// caller-supplied authz snapshot (the D2-full native decision engine).
    fn deny_test_service_with_snapshot(
        snapshot: crate::runtime::authz::AuthzSnapshot,
    ) -> AuthnServiceImpl {
        let cell = std::sync::Arc::new(arc_swap::ArcSwap::from_pointee(snapshot));
        deny_test_service().with_authz_snapshot(Some(cell))
    }

    #[tokio::test]
    async fn create_user_denied_by_explicit_native_authz_policy() {
        // D2-full: the per-action NATIVE authz decision engine denies CreateUser
        // when an explicit Deny policy governs the synthesized RPC resource — even
        // though the caller passed the coarse transport gate AND carries the action
        // scope. Proves the decision engine (not just the scope) is consulted.
        use crate::runtime::authz::{AuthzPolicy, AuthzSnapshot, Effect};
        let deny = AuthzPolicy {
            id: "deny-create-user".to_string(),
            priority: 100,
            enabled: true,
            effect: Effect::Deny,
            action: "authn.user.create".to_string(),
            // CreateUser's endpoint_security declares `decision_resource:
            // "authn.CreateUser"`, so the engine authorizes against THAT resource
            // (not the synthetic `native.rpc:` form). The deny policy must target it.
            resource: "authn.CreateUser".to_string(),
            ..AuthzPolicy::default()
        };
        let snapshot = AuthzSnapshot {
            version: "test-v1".to_string(),
            policies: vec![deny],
            ..AuthzSnapshot::default()
        };
        let svc = deny_test_service_with_snapshot(snapshot);
        // Same-tenant body so the body-tenant guard does NOT fire first; the caller
        // carries the action scope so the scope gate passes — only the native
        // decision can deny here.
        let ctx = crate::runtime::service::method_security::test_claim_context(
            "admin-a",
            "tenant-a",
            "",
            &["authn.user.create"],
            &[],
        );
        let req = Request::new(authn_pb::CreateUserRequest {
            username: "victim".to_string(),
            email: "victim@example.com".to_string(),
            password: "CorrectHorse1!".to_string(),
            tenant_id: "tenant-a".to_string(),
            ..Default::default()
        });
        let err = crate::runtime::service::method_security::scope_claim_context_for_test(
            ctx,
            svc.create_user_impl(req),
        )
        .await
        .expect_err("explicit native authz deny must block create_user");
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
        assert!(
            err.message().contains("denied by authz policy"),
            "deny should surface the native authz reason, got: {}",
            err.message()
        );
    }

    #[tokio::test]
    async fn create_user_denies_cross_tenant_body_even_with_action_scope() {
        // D1/D2: a tenant-A admin (carrying the create action scope) cannot mint a
        // tenant-B user. The body-tenant guard denies before any DB access.
        let svc = deny_test_service();
        let ctx = crate::runtime::service::method_security::test_claim_context(
            "admin-a",
            "tenant-a",
            "",
            &["authn.user.create"],
            &[],
        );
        let req = Request::new(authn_pb::CreateUserRequest {
            username: "victim".to_string(),
            email: "victim@example.com".to_string(),
            password: "CorrectHorse1!".to_string(),
            tenant_id: "tenant-b".to_string(),
            ..Default::default()
        });
        let err = crate::runtime::service::method_security::scope_claim_context_for_test(
            ctx,
            svc.create_user_impl(req),
        )
        .await
        .expect_err("cross-tenant create_user must be denied");
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    #[tokio::test]
    async fn create_user_created_by_mismatch_carries_policy_detail() {
        let svc = deny_test_service();
        let ctx = crate::runtime::service::method_security::test_claim_context(
            "admin-a",
            "tenant-a",
            "",
            &["authn.user.create"],
            &[],
        );
        let req = Request::new(authn_pb::CreateUserRequest {
            username: "victim".to_string(),
            email: "victim@example.com".to_string(),
            password: "CorrectHorse1!".to_string(),
            tenant_id: "tenant-a".to_string(),
            context: Some(crate::proto::udb::core::common::v1::RequestContext {
                principal_id: "other-admin".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        });
        let err = crate::runtime::service::method_security::scope_claim_context_for_test(
            ctx,
            svc.create_user_impl(req),
        )
        .await
        .expect_err("created_by mismatch must be denied before store access");
        assert_policy_detail(
            &err,
            "create_user",
            "created_by_principal_mismatch",
            "created_by principal must match the authenticated bearer subject",
        );
    }

    #[tokio::test]
    async fn create_user_denies_caller_without_action_scope() {
        // D2: authenticated, same-tenant, but no concrete action scope and no broad
        // admin scope → the per-action guard denies (coarse gate was the only check
        // before this fix).
        let svc = deny_test_service();
        let ctx = crate::runtime::service::method_security::test_claim_context(
            "user-a",
            "tenant-a",
            "",
            &["udb:authn:read"],
            &[],
        );
        let req = Request::new(authn_pb::CreateUserRequest {
            username: "newbie".to_string(),
            email: "newbie@example.com".to_string(),
            password: "CorrectHorse1!".to_string(),
            tenant_id: "tenant-a".to_string(),
            ..Default::default()
        });
        let err = crate::runtime::service::method_security::scope_claim_context_for_test(
            ctx,
            svc.create_user_impl(req),
        )
        .await
        .expect_err("missing action scope must be denied");
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    #[tokio::test]
    async fn read_tenant_filter_binds_empty_request_to_claim_tenant() {
        let ctx = crate::runtime::service::method_security::test_claim_context(
            "reader-a",
            "tenant-a",
            "",
            &["udb:authn:read"],
            &[],
        );
        let tenant =
            crate::runtime::service::method_security::scope_claim_context_for_test(ctx, async {
                claim_bound_read_tenant("")
            })
            .await
            .expect("claim tenant should bind an empty read tenant");
        assert_eq!(tenant.as_deref(), Some("tenant-a"));
    }

    #[tokio::test]
    async fn read_tenant_filter_denies_cross_tenant_request_for_non_admin() {
        let ctx = crate::runtime::service::method_security::test_claim_context(
            "reader-a",
            "tenant-a",
            "",
            &["udb:authn:read"],
            &[],
        );
        let err =
            crate::runtime::service::method_security::scope_claim_context_for_test(ctx, async {
                claim_bound_read_tenant("tenant-b")
            })
            .await
            .expect_err("non-admin read must stay in the claim tenant");
        assert_policy_detail(
            &err,
            "authn_user_read",
            "tenant_mismatch",
            "request tenant must match the bearer token tenant",
        );
    }

    #[tokio::test]
    async fn read_tenant_filter_denies_tenantless_non_admin() {
        let ctx = crate::runtime::service::method_security::test_claim_context(
            "reader-a",
            "",
            "",
            &["udb:authn:read"],
            &[],
        );
        let err =
            crate::runtime::service::method_security::scope_claim_context_for_test(ctx, async {
                claim_bound_read_tenant("")
            })
            .await
            .expect_err("non-admin read must have a tenant-bound bearer");
        assert_policy_detail(
            &err,
            "authn_user_read",
            "tenant_scoped_bearer_required",
            "operation requires a tenant-scoped bearer token or a cross-tenant admin role",
        );
    }

    #[tokio::test]
    async fn read_tenant_filter_allows_empty_tenant_for_cross_tenant_admin() {
        let ctx = crate::runtime::service::method_security::test_claim_context(
            "platform-admin",
            "",
            "",
            &[],
            &["platform_admin"],
        );
        let tenant =
            crate::runtime::service::method_security::scope_claim_context_for_test(ctx, async {
                claim_bound_read_tenant("")
            })
            .await
            .expect("cross-tenant admin may perform an unfiltered read");
        assert!(tenant.is_none());
    }

    /// Read-only [`UserStore`] double serving ONE canned record, so the
    /// user-resolution tests can drive the REAL serving path (`get_user_impl` /
    /// `list_users_impl` → `claim_bound_read_tenant` → the store's in-tenant
    /// lookups — the trait's actual default tenant filters in `runtime::authn`)
    /// without a live Postgres. `list_users` captures the tenant the handler
    /// bound, so the claim-tenant binding is asserted against the real store
    /// call rather than re-derived; the double itself never filters, so a
    /// filtered outcome can only come from the serving path. Every mutation
    /// fails closed. The Postgres store's SQL override of the in-tenant lookups
    /// is covered by the env-gated live suite (`tests/authn_user_live.rs`).
    struct CannedUserStore {
        record: UserRecord,
        listed_tenants: std::sync::Mutex<Vec<String>>,
    }

    impl CannedUserStore {
        fn new(record: UserRecord) -> Self {
            Self {
                record,
                listed_tenants: std::sync::Mutex::new(Vec::new()),
            }
        }

        fn listed_tenants(&self) -> Vec<String> {
            self.listed_tenants
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone()
        }
    }

    #[async_trait::async_trait]
    impl crate::runtime::authn::UserStore for CannedUserStore {
        async fn put_user(&self, _record: UserRecord) -> Result<(), String> {
            Err("read-only canned user store (test double)".to_string())
        }

        async fn get_user_by_id(&self, user_id: &str) -> Result<Option<UserRecord>, String> {
            Ok((user_id == self.record.user_id).then(|| self.record.clone()))
        }

        async fn get_user_by_username(&self, username: &str) -> Result<Option<UserRecord>, String> {
            Ok((username == self.record.username).then(|| self.record.clone()))
        }

        async fn get_user_by_email(&self, email: &str) -> Result<Option<UserRecord>, String> {
            Ok((email == self.record.email).then(|| self.record.clone()))
        }

        async fn list_users(
            &self,
            tenant_id: &str,
            _account_kind: crate::runtime::authn::AccountKind,
            _status: crate::runtime::authn::AccountStatus,
        ) -> Result<Vec<UserRecord>, String> {
            self.listed_tenants
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(tenant_id.to_string());
            Ok(vec![self.record.clone()])
        }

        async fn delete_user(
            &self,
            _user_id: &str,
            _deleted_by: &str,
            _now_unix: u64,
        ) -> Result<bool, String> {
            Err("read-only canned user store (test double)".to_string())
        }

        async fn put_otp(&self, _record: crate::runtime::authn::OtpRecord) -> Result<(), String> {
            Err("read-only canned user store (test double)".to_string())
        }

        async fn get_otp(
            &self,
            _otp_id: &str,
        ) -> Result<Option<crate::runtime::authn::OtpRecord>, String> {
            Err("read-only canned user store (test double)".to_string())
        }

        async fn update_otp(
            &self,
            _record: crate::runtime::authn::OtpRecord,
        ) -> Result<(), String> {
            Err("read-only canned user store (test double)".to_string())
        }
    }

    fn user_record_in_tenant(tenant: &str) -> UserRecord {
        UserRecord {
            tenant_id: tenant.to_string(),
            ..user_record()
        }
    }

    /// Service over the canned read-only user store: the claim context plus the
    /// store's real default tenant filters do all the work (no Postgres).
    fn read_test_service(users: std::sync::Arc<CannedUserStore>) -> AuthnServiceImpl {
        let config = crate::runtime::authn::AuthnConfig {
            session_hash_secret: "deny-test-secret".to_string(),
            ..crate::runtime::authn::AuthnConfig::default()
        };
        AuthnServiceImpl::with_stores(
            config,
            crate::runtime::security::SecurityConfig::default(),
            std::sync::Arc::new(crate::runtime::authn::UnavailableSessionStore),
            std::sync::Arc::new(crate::runtime::authn::UnavailableApiKeyStore),
            users,
        )
    }

    #[tokio::test]
    async fn get_user_hides_cross_tenant_target_as_not_found() {
        // TEST-6: a tenant-A reader resolving a tenant-B user through the REAL
        // lookup-filter path (`get_user_impl` → `claim_bound_read_tenant` →
        // `get_user_by_id_in_tenant`) gets NOT_FOUND — the foreign record never
        // leaves the handler.
        let store = std::sync::Arc::new(CannedUserStore::new(user_record_in_tenant("tenant-b")));
        let svc = read_test_service(store);
        let ctx = crate::runtime::service::method_security::test_claim_context(
            "reader-a",
            "tenant-a",
            "",
            &["udb:authn:read"],
            &[],
        );
        let err = crate::runtime::service::method_security::scope_claim_context_for_test(
            ctx,
            svc.get_user_impl(Request::new(authn_pb::GetUserRequest {
                user_id: "user-1".to_string(),
                ..Default::default()
            })),
        )
        .await
        .expect_err("a tenant-B user must be NOT_FOUND for a tenant-A reader");
        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn get_user_resolves_same_tenant_target() {
        // Positive control for the cross-tenant test above: the SAME claim
        // context and the SAME lookup succeed when the record IS in the claim
        // tenant — proving the NOT_FOUND comes from the tenant filter, not the
        // test scaffolding.
        let store = std::sync::Arc::new(CannedUserStore::new(user_record_in_tenant("tenant-a")));
        let svc = read_test_service(store);
        let ctx = crate::runtime::service::method_security::test_claim_context(
            "reader-a",
            "tenant-a",
            "",
            &["udb:authn:read"],
            &[],
        );
        let user = crate::runtime::service::method_security::scope_claim_context_for_test(
            ctx,
            svc.get_user_impl(Request::new(authn_pb::GetUserRequest {
                user_id: "user-1".to_string(),
                ..Default::default()
            })),
        )
        .await
        .expect("same-tenant get_user must resolve")
        .into_inner()
        .user
        .expect("resolved user");
        assert_eq!(user.user_id, "user-1");
        assert_eq!(user.tenant_id, "tenant-a");
    }

    #[tokio::test]
    async fn list_users_binds_empty_request_tenant_to_claim_tenant() {
        // TEST-6: an empty-tenant list from a tenant-bound reader reaches the
        // store ALREADY bound to the claim tenant (asserted on the captured
        // store call), never as an unfiltered cross-tenant list.
        let store = std::sync::Arc::new(CannedUserStore::new(user_record_in_tenant("tenant-a")));
        let svc = read_test_service(store.clone());
        let ctx = crate::runtime::service::method_security::test_claim_context(
            "reader-a",
            "tenant-a",
            "",
            &["udb:authn:read"],
            &[],
        );
        let listed = crate::runtime::service::method_security::scope_claim_context_for_test(
            ctx,
            svc.list_users_impl(Request::new(authn_pb::ListUsersRequest::default())),
        )
        .await
        .expect("tenant-bound list_users must succeed")
        .into_inner();
        assert_eq!(
            store.listed_tenants(),
            vec!["tenant-a".to_string()],
            "the store lookup must be bound to the claim tenant"
        );
        assert_eq!(listed.users.len(), 1);
    }

    #[tokio::test]
    async fn list_users_denies_cross_tenant_request_before_store_access() {
        // TEST-6: a tenant-A reader requesting a tenant-B list is denied by the
        // real handler BEFORE any store access (fail closed).
        let store = std::sync::Arc::new(CannedUserStore::new(user_record_in_tenant("tenant-a")));
        let svc = read_test_service(store.clone());
        let ctx = crate::runtime::service::method_security::test_claim_context(
            "reader-a",
            "tenant-a",
            "",
            &["udb:authn:read"],
            &[],
        );
        let err = crate::runtime::service::method_security::scope_claim_context_for_test(
            ctx,
            svc.list_users_impl(Request::new(authn_pb::ListUsersRequest {
                tenant_id: "tenant-b".to_string(),
                ..Default::default()
            })),
        )
        .await
        .expect_err("cross-tenant list_users must be denied");
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
        assert!(
            store.listed_tenants().is_empty(),
            "the deny must fire before any store access"
        );
    }

    #[test]
    fn user_mapper_blanks_descriptor_storage_only_fields() {
        let storage_only = storage_only_field_names("udb.core.authn.entity.v1.User");
        assert_eq!(
            storage_only,
            ["password_hash", "totp_secret_enc"]
                .into_iter()
                .map(str::to_string)
                .collect()
        );

        let pb = user_record_to_pb(&user_record());
        for field in storage_only {
            assert!(
                user_pb_string_field(&pb, &field).is_empty(),
                "descriptor storage-only User field {field} must be blanked by the mapper"
            );
        }
    }
}

fn claim_bound_read_tenant(request_tenant: &str) -> Result<Option<String>, Status> {
    let request_tenant = request_tenant.trim();
    if !crate::runtime::service::method_security::claim_context_present() {
        return Ok((!request_tenant.is_empty()).then(|| request_tenant.to_string()));
    }

    let ctx = crate::runtime::service::method_security::current_claim_context();
    if ctx.is_cross_tenant_admin() {
        return Ok((!request_tenant.is_empty()).then(|| request_tenant.to_string()));
    }

    let claim_tenant = ctx.tenant_id.trim();
    if claim_tenant.is_empty() {
        tracing::warn!(
            target: "udb.audit.authz",
            subject = %ctx.subject,
            "DENY: authn read requires a tenant-bound bearer or cross-tenant admin"
        );
        return Err(authn_read_tenant_scope_required_status());
    }

    if !request_tenant.is_empty() && request_tenant != claim_tenant {
        tracing::warn!(
            target: "udb.audit.authz",
            subject = %ctx.subject,
            claim_tenant = %claim_tenant,
            request_tenant = %request_tenant,
            "DENY: authn read tenant does not match the validated bearer tenant"
        );
        return Err(authn_read_tenant_mismatch_status());
    }

    Ok(Some(claim_tenant.to_string()))
}

impl AuthnServiceImpl {
    pub(super) async fn create_user_impl(
        &self,
        request: Request<authn_pb::CreateUserRequest>,
    ) -> Result<Response<authn_pb::CreateUserResponse>, Status> {
        if self.password_hash_key().is_empty() {
            return Err(create_user_password_secret_status());
        }
        let req = request.into_inner();
        let mut missing_identity = Vec::new();
        if req.username.trim().is_empty() {
            missing_identity.push(("username", "must be a non-empty username"));
        }
        if req.email.trim().is_empty() {
            missing_identity.push(("email", "must be a non-empty email address"));
        }
        if !missing_identity.is_empty() {
            return Err(authn_core_invalid_fields(
                "username and email are required",
                missing_identity,
            ));
        }
        // D2: per-action authz beyond the coarse transport `udb:admin` gate, plus
        // D1: the new user's body tenant/project must match the validated bearer
        // tenant/project (a tenant-A admin token cannot mint a tenant-B user) unless
        // the caller is a cross-tenant admin. Both fail closed + audit on deny.
        let claim_ctx = crate::runtime::service::method_security::current_claim_context();
        crate::runtime::service::method_security::authorize_action(
            &claim_ctx,
            "authn.user.create",
            &["authn.user.create", "authn.user.write", "udb:authn:admin"],
        )?;
        // D2-full: per-action NATIVE authz decision via the shared decision engine
        // (the same AuthzSnapshot the AuthzService decides against). Denies on an
        // explicit policy deny; returns the engine's real `decision_id` to thread
        // into the audit compliance envelope below.
        let decision_id = self
            .decide_action_native(&claim_ctx, AUTHN_CREATE_USER_PATH, "authn.user.create")
            .await?;
        // 01.4.2.1 — resolve the tenant/project to STORE from the verified claim
        // (validate-equal on a non-empty body; inherit the claim tenant/project on
        // an omitted body so the row is visible to claim-bound reads instead of
        // being stored as ""). Same fail-closed semantics as the prior
        // enforce_body_tenant_matches_claim call.
        let (tenant_id, project_id) =
            crate::runtime::service::method_security::resolve_body_tenant_scope(
                &claim_ctx,
                &req.tenant_id,
                &req.project_id,
            )?;
        // Externally-provisioned (SSO/OIDC) users have no local password to vet.
        if req.external_provider_id.trim().is_empty() {
            authn::PasswordPolicy::from_env()
                .validate(&req.password)
                .map_err(create_user_password_policy_status)?;
        }
        let now = now_unix();
        let user_id = Uuid::new_v4().to_string();
        // Default an unspecified request to PERSON, then cross the adapter boundary
        // into the domain enum so the record never carries a proto integer.
        let account_kind = match account_kind_from_proto(req.account_kind) {
            crate::runtime::authn::AccountKind::Unspecified => {
                crate::runtime::authn::AccountKind::Person
            }
            kind => kind,
        };
        // 01.4.3.1 — attribution comes from the verified claim, not a forgeable body
        // principal. Default an omitted `created_by` to the bearer subject; a body
        // principal that disagrees with the bearer (and is not a cross-tenant /
        // impersonation admin) is rejected so attribution cannot be forged. Skipped
        // on the in-process loopback path where no claim context is installed.
        let body_principal = req
            .context
            .as_ref()
            .map(|ctx| ctx.principal_id.trim().to_string())
            .unwrap_or_default();
        let created_by = if !crate::runtime::service::method_security::claim_context_present() {
            body_principal
        } else if body_principal.is_empty() {
            claim_ctx.subject.clone()
        } else if body_principal.as_str() != claim_ctx.subject.trim()
            && !claim_ctx.is_cross_tenant_admin()
        {
            return Err(created_by_principal_mismatch_status());
        } else {
            body_principal
        };
        let rec = UserRecord {
            user_id: user_id.clone(),
            username: req.username.trim().to_ascii_lowercase(),
            email: req.email.trim().to_ascii_lowercase(),
            password_hash: authn::hash_password(&req.password, &self.password_hash_key()),
            account_kind,
            status: crate::runtime::authn::AccountStatus::PendingVerification,
            tenant_id,
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
            project_id,
            external_provider_id: req.external_provider_id,
            external_subject: req.external_subject,
            profile_attributes_json: serde_json::to_string(&req.profile_attributes)
                .unwrap_or_else(|_| "{}".to_string()),
        };
        // Prepare the email-verification OTP (record + plaintext code) WITHOUT
        // persisting or sending it yet, so the row can land inside the same
        // transaction as the user upsert and the registration event. The
        // notification is a side-effect that cannot be rolled back, so it is sent
        // only AFTER the transaction commits (best-effort, post-commit).
        let (otp_rec, otp_code) = self.prepare_otp_record(
            &rec,
            authn_entity_pb::OtpType::EmailVerification as i32,
            "email",
            &rec.email,
            format!("create_user:{user_id}"),
            now,
        );
        let otp_id = otp_rec.otp_id.clone();
        let otp_channel = otp_rec.delivery_channel.clone();
        let otp_address = otp_rec.delivery_address.clone();
        let event = AuthEvent::new(
            topics::USER_REGISTERED,
            rec.user_id.clone(),
            rec.tenant_id.clone(),
            serde_json::json!({
                "user_id": rec.user_id.clone(),
                "username": rec.username.clone(),
                "email": rec.email.clone(),
                "tenant_id": rec.tenant_id.clone(),
                "project_id": rec.project_id.clone(),
                "account_kind": account_kind_to_proto(rec.account_kind),
                "created_by": rec.created_by.clone(),
            }),
        )
        .with_correlation(format!("create_user:{user_id}"))
        // D2-full: link the native authz decision to the audit record so the
        // compliance envelope carries the real per-action `decision_id`.
        .with_compliance(ComplianceEnvelope {
            actor: claim_ctx.subject.clone(),
            actor_project: claim_ctx.project_id.clone(),
            target_resource: rec.user_id.clone(),
            target_tenant: rec.tenant_id.clone(),
            target_project: rec.project_id.clone(),
            operation: "create".to_string(),
            outcome: "success".to_string(),
            reason_code: "authz_allow".to_string(),
            decision_id: decision_id.clone(),
            ..ComplianceEnvelope::default()
        });
        // Atomicity (§7): the user upsert, its email-verification OTP row, and the
        // USER_REGISTERED audit/outbox event commit in ONE transaction — a failure
        // at any step (incl. the audit write) rolls the whole creation back rather
        // than leaving a user without its OTP or without its event.
        let pool = self.require_pool()?;
        let mut tx = pool.begin().await.map_err(|err| {
            authn_core_internal_status(
                "create_user_tx_begin",
                format!("create user tx begin failed: {err}"),
            )
        })?;
        self.users
            .put_user_in_tx(&mut *tx, rec.clone())
            .await
            .map_err(crate::runtime::executor_utils::status_from_store_string)?;
        self.users
            .put_otp_in_tx(&mut *tx, otp_rec)
            .await
            .map_err(crate::runtime::executor_utils::status_from_store_string)?;
        self.emit_event_in_tx(&mut *tx, event).await?;
        tx.commit().await.map_err(|err| {
            authn_core_internal_status(
                "create_user_tx_commit",
                format!("create user commit failed: {err}"),
            )
        })?;
        // Post-commit, best-effort OTP delivery (cannot be rolled back; the OTP is
        // already durable and the caller holds the otp_id).
        self.deliver_otp_code(
            &otp_channel,
            &otp_address,
            &otp_code,
            authn_entity_pb::OtpType::EmailVerification as i32,
            &rec.user_id,
            &otp_id,
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
        let has_user_id = !req.user_id.trim().is_empty();
        let has_username = !req.username.trim().is_empty();
        let has_email = !req.email.trim().is_empty();
        if !has_user_id && !has_username && !has_email {
            return Err(authn_core_invalid_fields(
                "one of user_id, username, or email is required",
                [
                    ("user_id", "must include at least one lookup identity"),
                    ("username", "must include at least one lookup identity"),
                    ("email", "must include at least one lookup identity"),
                ],
            ));
        }
        let tenant = claim_bound_read_tenant("")?;
        let user = if has_user_id {
            if let Some(tenant) = tenant.as_deref() {
                self.users
                    .get_user_by_id_in_tenant(&req.user_id, tenant)
                    .await
            } else {
                self.users.get_user_by_id(&req.user_id).await
            }
        } else if has_username {
            let username = req.username.to_ascii_lowercase();
            if let Some(tenant) = tenant.as_deref() {
                self.users
                    .get_user_by_username_in_tenant(&username, tenant)
                    .await
            } else {
                self.users.get_user_by_username(&username).await
            }
        } else {
            let email = req.email.to_ascii_lowercase();
            if let Some(tenant) = tenant.as_deref() {
                self.users.get_user_by_email_in_tenant(&email, tenant).await
            } else {
                self.users.get_user_by_email(&email).await
            }
        }
        .map_err(|err| authn_core_internal_status("get_user", err))?
        .ok_or_else(super::authn_user_not_found_status)?;
        Ok(Response::new(authn_pb::GetUserResponse {
            user: Some(user_record_to_pb(&user)),
        }))
    }

    pub(super) async fn list_users_impl(
        &self,
        request: Request<authn_pb::ListUsersRequest>,
    ) -> Result<Response<authn_pb::ListUsersResponse>, Status> {
        let req = request.into_inner();
        let tenant = claim_bound_read_tenant(&req.tenant_id)?;
        let page = req.page.as_ref();
        let (limit, offset, _) = bounded_page_window(page);
        let (users, total) = self
            .users
            .list_users_page(
                tenant.as_deref().unwrap_or(""),
                account_kind_from_proto(req.account_kind),
                account_status_from_proto(req.status),
                limit,
                offset,
            )
            .await
            .map_err(|err| authn_core_internal_status("list_users", err))?;
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
        // D2 + D1: require a concrete update action scope (beyond the coarse
        // transport gate), then bind the operation to the validated bearer tenant.
        let claim_ctx = crate::runtime::service::method_security::current_claim_context();
        crate::runtime::service::method_security::authorize_action(
            &claim_ctx,
            "authn.user.update",
            &["authn.user.update", "authn.user.write", "udb:authn:admin"],
        )?;
        // D2-full: per-action native authz decision (denies on an explicit policy
        // deny). UpdateUser emits no domain event, so the decision_id is not
        // threaded into an envelope here; the decision still gates the mutation.
        let _decision_id = self
            .decide_action_native(&claim_ctx, AUTHN_UPDATE_USER_PATH, "authn.user.update")
            .await?;
        let mut rec = self
            .users
            .get_user_by_id(&req.user_id)
            .await
            .map_err(|err| authn_core_internal_status("update_user_load", err))?
            .ok_or_else(super::authn_user_not_found_status)?;
        // The EXISTING user must be in the caller's tenant (a tenant-A admin cannot
        // edit a tenant-B user). Cross-tenant admins bypass.
        crate::runtime::service::method_security::enforce_body_tenant_matches_claim(
            &claim_ctx,
            &rec.tenant_id,
            &rec.project_id,
        )?;
        // If the request RE-TARGETS the user's tenant/project, the new values must
        // also be within the caller's authority — otherwise a tenant-A admin could
        // move a user into tenant-B. Cross-tenant admins may relocate freely.
        if !req.tenant_id.trim().is_empty() || !req.project_id.trim().is_empty() {
            crate::runtime::service::method_security::enforce_body_tenant_matches_claim(
                &claim_ctx,
                &req.tenant_id,
                &req.project_id,
            )?;
        }
        let update_mask = crate::runtime::service::native_helpers::update_mask_path_set(
            req.update_mask.as_ref(),
            &[
                "full_name",
                "email",
                "tenant_id",
                "account_kind",
                "project_id",
                "profile_attributes",
                "external_provider_id",
                "external_subject",
            ],
        )?;
        if crate::runtime::service::native_helpers::update_mask_allows(
            &update_mask,
            "full_name",
            !req.full_name.trim().is_empty(),
        ) {
            rec.full_name = req.full_name;
        }
        if crate::runtime::service::native_helpers::update_mask_allows(
            &update_mask,
            "email",
            !req.email.trim().is_empty(),
        ) {
            rec.email = req.email.trim().to_ascii_lowercase();
        }
        if crate::runtime::service::native_helpers::update_mask_allows(
            &update_mask,
            "tenant_id",
            !req.tenant_id.trim().is_empty(),
        ) {
            rec.tenant_id = req.tenant_id;
        }
        if crate::runtime::service::native_helpers::update_mask_allows(
            &update_mask,
            "account_kind",
            req.account_kind != authn_entity_pb::AccountKind::Unspecified as i32,
        ) {
            rec.account_kind = account_kind_from_proto(req.account_kind);
        }
        if crate::runtime::service::native_helpers::update_mask_allows(
            &update_mask,
            "project_id",
            !req.project_id.trim().is_empty(),
        ) {
            rec.project_id = req.project_id;
        }
        if crate::runtime::service::native_helpers::update_mask_allows(
            &update_mask,
            "external_provider_id",
            !req.external_provider_id.trim().is_empty(),
        ) {
            rec.external_provider_id = req.external_provider_id;
        }
        if crate::runtime::service::native_helpers::update_mask_allows(
            &update_mask,
            "external_subject",
            !req.external_subject.trim().is_empty(),
        ) {
            rec.external_subject = req.external_subject;
        }
        if crate::runtime::service::native_helpers::update_mask_allows(
            &update_mask,
            "profile_attributes",
            !req.profile_attributes.is_empty(),
        ) {
            rec.profile_attributes_json =
                serde_json::to_string(&req.profile_attributes).unwrap_or_else(|_| "{}".to_string());
        }
        rec.updated_at_unix = now_unix();
        self.users
            .put_user(rec.clone())
            .await
            .map_err(crate::runtime::executor_utils::status_from_store_string)?;
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
            return Err(authn_core_invalid_fields(
                "new_status is required",
                [("new_status", "must specify a target user status")],
            ));
        }
        // D2: status changes are privileged; require a concrete action scope.
        let claim_ctx = crate::runtime::service::method_security::current_claim_context();
        crate::runtime::service::method_security::authorize_action(
            &claim_ctx,
            "authn.user.status.write",
            &[
                "authn.user.status.write",
                "authn.user.write",
                "udb:authn:admin",
            ],
        )?;
        // D2-full: per-action native authz decision; decision_id is threaded into
        // the USER_STATUS_CHANGED compliance envelope below.
        let decision_id = self
            .decide_action_native(
                &claim_ctx,
                AUTHN_CHANGE_USER_STATUS_PATH,
                "authn.user.status.write",
            )
            .await?;
        let mut rec = self
            .users
            .get_user_by_id(&req.user_id)
            .await
            .map_err(|err| authn_core_internal_status("change_user_status_load", err))?
            .ok_or_else(super::authn_user_not_found_status)?;
        // D1: the target user must be in the caller's tenant (cross-tenant admins
        // bypass) — a tenant-A admin cannot suspend/lock a tenant-B user.
        crate::runtime::service::method_security::enforce_body_tenant_matches_claim(
            &claim_ctx,
            &rec.tenant_id,
            &rec.project_id,
        )?;
        let old_status = account_status_to_proto(rec.status);
        rec.status = account_status_from_proto(req.new_status);
        rec.updated_at_unix = now_unix();
        let event = AuthEvent::new(
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
        )
        .with_correlation(format!("change_user_status:{}", rec.user_id))
        // D2-full: link the native authz decision to the status-change audit row.
        .with_compliance(ComplianceEnvelope {
            actor: claim_ctx.subject.clone(),
            actor_project: claim_ctx.project_id.clone(),
            target_resource: rec.user_id.clone(),
            target_tenant: rec.tenant_id.clone(),
            target_project: rec.project_id.clone(),
            operation: "status_change".to_string(),
            outcome: "success".to_string(),
            reason_code: "authz_allow".to_string(),
            decision_id: decision_id.clone(),
            ..ComplianceEnvelope::default()
        });
        // Atomicity (§7): the user-status UPDATE and its audit event commit in one
        // transaction on the shared auth Postgres pool.
        let pool = self.require_pool()?;
        let mut tx = pool.begin().await.map_err(|err| {
            authn_core_internal_status(
                "change_user_status_tx_begin",
                format!("status change tx begin failed: {err}"),
            )
        })?;
        self.users
            .put_user_in_tx(&mut *tx, rec.clone())
            .await
            .map_err(|err| authn_core_internal_status("change_user_status_store", err))?;
        self.emit_event_in_tx(&mut *tx, event).await?;
        tx.commit().await.map_err(|err| {
            authn_core_internal_status(
                "change_user_status_tx_commit",
                format!("status change commit failed: {err}"),
            )
        })?;
        Ok(Response::new(authn_pb::ChangeUserStatusResponse {
            user: Some(user_record_to_pb(&rec)),
        }))
    }

    pub(super) async fn admin_reset_password_impl(
        &self,
        request: Request<authn_pb::AdminResetPasswordRequest>,
    ) -> Result<Response<authn_pb::AdminResetPasswordResponse>, Status> {
        let req = request.into_inner();
        // D2: admin password reset is highly privileged; require a concrete action
        // scope beyond the coarse transport gate.
        let claim_ctx = crate::runtime::service::method_security::current_claim_context();
        crate::runtime::service::method_security::authorize_action(
            &claim_ctx,
            "authn.user.password.reset",
            &[
                "authn.user.password.reset",
                "authn.user.write",
                "udb:authn:admin",
            ],
        )?;
        // D2-full: per-action native authz decision (denies on an explicit policy
        // deny). The reset issues an OTP whose event is emitted inside `issue_otp`;
        // the decision still gates this highly-privileged mutation.
        let _decision_id = self
            .decide_action_native(
                &claim_ctx,
                AUTHN_ADMIN_RESET_PASSWORD_PATH,
                "authn.user.password.reset",
            )
            .await?;
        let user = self
            .users
            .get_user_by_id(&req.user_id)
            .await
            .map_err(|err| authn_core_internal_status("admin_reset_password_load", err))?
            .ok_or_else(super::authn_user_not_found_status)?;
        // D1: the target user must be in the caller's tenant (cross-tenant admins
        // bypass) — a tenant-A admin cannot trigger a reset for a tenant-B user.
        crate::runtime::service::method_security::enforce_body_tenant_matches_claim(
            &claim_ctx,
            &user.tenant_id,
            &user.project_id,
        )?;
        let (otp_id, _code) = self
            .issue_otp(
                &user,
                authn_entity_pb::OtpType::PasswordReset as i32,
                format!("admin_reset_password:{}", user.user_id),
                now_unix(),
            )
            .await?;
        Ok(Response::new(authn_pb::AdminResetPasswordResponse {
            otp_id,
        }))
    }
}
