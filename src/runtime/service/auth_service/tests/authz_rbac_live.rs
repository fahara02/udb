use super::support::*;
use crate::proto::udb::core::authn::services::v1 as authn_pb;
use crate::proto::udb::core::authn::services::v1::authn_service_server::AuthnService;
use crate::proto::udb::core::authz::services::v1 as authz_pb;
use crate::proto::udb::core::authz::services::v1::authz_service_server::AuthzService;
use tonic::Request;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_authz_role_policy_roundtrip -- --ignored --nocapture"]
async fn live_postgres_authz_role_policy_roundtrip() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let authn = authn_service(pool.clone());
    let authz = authz_service(pool.clone());
    let suffix = Uuid::new_v4().simple().to_string();

    let created = authn
        .create_user(Request::new(authn_pb::CreateUserRequest {
            username: format!("rbac_{suffix}"),
            email: format!("rbac_{suffix}@example.com"),
            password: "CorrectHorse1!".to_string(),
            tenant_id: "acme".to_string(),
            full_name: "RBAC Live".to_string(),
            project_id: "billing".to_string(),
            ..Default::default()
        }))
        .await
        .expect("create Postgres user for authz FK")
        .into_inner();
    let user = created.user.expect("created user");

    let role = authz
        .create_role(Request::new(authz_pb::CreateRoleRequest {
            name: format!("Reader {suffix}"),
            description: "Live DB reader role".to_string(),
            created_by: user.user_id.clone(),
            role_code: format!("reader_{suffix}"),
            domain: "acme".to_string(),
            tenant_id: "acme".to_string(),
            project_id: "billing".to_string(),
            ..Default::default()
        }))
        .await
        .expect("create role in Postgres")
        .into_inner()
        .role
        .expect("role");

    let expires_at = now_unix() + 3600;
    let user_role = authz
        .assign_role(Request::new(authz_pb::AssignRoleRequest {
            user_id: user.user_id.clone(),
            role_id: role.role_id.clone(),
            domain: "acme".to_string(),
            assigned_by: user.user_id.clone(),
            expires_at: Some(prost_types::Timestamp {
                seconds: expires_at as i64,
                nanos: 0,
            }),
            tenant_id: "acme".to_string(),
            project_id: "billing".to_string(),
            ..Default::default()
        }))
        .await
        .expect("assign role in Postgres")
        .into_inner()
        .user_role
        .expect("user role");
    assert_eq!(user_role.user_id, user.user_id);
    assert_eq!(
        user_role.expires_at.as_ref().map(|ts| ts.seconds),
        Some(expires_at as i64)
    );

    let policy_id = Uuid::new_v4().to_string();
    authz
        .put_authz_policy(Request::new(authz_pb::PutAuthzPolicyRequest {
            policy: Some(authz_pb::AuthzPolicyRecord {
                id: policy_id.clone(),
                enabled: true,
                effect: "allow".to_string(),
                tenant: "acme".to_string(),
                project: "billing".to_string(),
                role: role.role_code.clone(),
                action: "data.select".to_string(),
                resource: "invoice".to_string(),
                ..Default::default()
            }),
        }))
        .await
        .expect("put Postgres authz policy");

    let allowed = authz
        .check_access(Request::new(authz_pb::CheckAccessRequest {
            user_id: user.user_id.clone(),
            domain: "acme".to_string(),
            tenant_id: "acme".to_string(),
            project_id: "billing".to_string(),
            object: "invoice".to_string(),
            action: "data.select".to_string(),
            ..Default::default()
        }))
        .await
        .expect("check access through Postgres snapshot")
        .into_inner();
    assert!(allowed.allowed);
    assert_eq!(allowed.matched_rule, policy_id);

    let listed_roles = authz
        .list_user_roles(Request::new(authz_pb::ListUserRolesRequest {
            user_id: user.user_id.clone(),
            domain: "acme".to_string(),
            active_only: true,
        }))
        .await
        .expect("list Postgres user roles")
        .into_inner();
    assert_eq!(listed_roles.user_roles.len(), 1);
    assert!(listed_roles.user_roles[0].expires_at.is_some());

    authz
        .revoke_role(Request::new(authz_pb::RevokeRoleRequest {
            user_role_id: user_role.user_role_id,
            user_id: user.user_id.clone(),
            reason: "live_test".to_string(),
            revoked_by: user.user_id.clone(),
        }))
        .await
        .expect("revoke Postgres role");

    let denied = authz
        .check_access(Request::new(authz_pb::CheckAccessRequest {
            user_id: user.user_id,
            domain: "acme".to_string(),
            tenant_id: "acme".to_string(),
            project_id: "billing".to_string(),
            object: "invoice".to_string(),
            action: "data.select".to_string(),
            ..Default::default()
        }))
        .await
        .expect("check access after revoke")
        .into_inner();
    assert!(!denied.allowed);

    cleanup_native_auth_db(&pool).await;
}
