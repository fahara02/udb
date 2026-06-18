use super::support::*;
use crate::proto::udb::core::authn::entity::v1 as authn_entity_pb;
use crate::proto::udb::core::authn::services::v1 as authn_pb;
use crate::proto::udb::core::authn::services::v1::authn_service_server::AuthnService;
use tonic::Request;

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_authn_user_account_lifecycle -- --ignored --nocapture"]
async fn live_postgres_authn_user_account_lifecycle() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let svc = authn_service(pool.clone());
    let user = create_verified_user(&svc, "account", "CorrectHorse1!").await;

    let by_email = svc
        .get_user(Request::new(authn_pb::GetUserRequest {
            email: user.email.clone(),
            ..Default::default()
        }))
        .await
        .expect("get user by email")
        .into_inner()
        .user
        .expect("user by email");
    assert_eq!(by_email.user_id, user.user_id);

    let updated = svc
        .update_user(Request::new(authn_pb::UpdateUserRequest {
            user_id: user.user_id.clone(),
            full_name: "Updated Live User".to_string(),
            project_id: "ledger".to_string(),
            profile_attributes: [("tier".to_string(), "gold".to_string())].into(),
            ..Default::default()
        }))
        .await
        .expect("update user")
        .into_inner()
        .user
        .expect("updated user");
    assert_eq!(updated.full_name, "Updated Live User");
    assert_eq!(updated.project_id, "ledger");
    assert!(updated.profile_attributes_json.contains("gold"));

    let listed = svc
        .list_users(Request::new(authn_pb::ListUsersRequest {
            tenant_id: "acme".to_string(),
            status: authn_entity_pb::UserStatus::Active as i32,
            ..Default::default()
        }))
        .await
        .expect("list active users")
        .into_inner();
    assert!(
        listed
            .users
            .iter()
            .any(|listed| listed.user_id == user.user_id)
    );

    let suspended = svc
        .change_user_status(Request::new(authn_pb::ChangeUserStatusRequest {
            user_id: user.user_id.clone(),
            new_status: authn_entity_pb::UserStatus::Suspended as i32,
            reason: "live_test".to_string(),
            ..Default::default()
        }))
        .await
        .expect("suspend user")
        .into_inner()
        .user
        .expect("suspended user");
    assert_eq!(
        suspended.status,
        authn_entity_pb::UserStatus::Suspended as i32
    );

    let suspended_login = svc
        .login(Request::new(authn_pb::LoginRequest {
            username: user.email,
            password: "CorrectHorse1!".to_string(),
            ..Default::default()
        }))
        .await
        .expect_err("suspended user must not login");
    assert_eq!(suspended_login.code(), tonic::Code::PermissionDenied);

    cleanup_native_auth_db(&pool).await;
}

/// §1 read-after-write (13.7.1.3): the `user_id` `CreateUser` returns is IMMEDIATELY
/// gettable by `GetUser` and listable by `ListUsers` on the SAME served path with the
/// SAME tenant metadata a client uses. Reverting the create/get guarantee fails this.
#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_authn_user_read_after_write -- --ignored --nocapture"]
async fn live_postgres_authn_user_read_after_write() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let svc = authn_service(pool.clone());
    let suffix = uuid::Uuid::new_v4().simple().to_string();

    let created = svc
        .create_user(Request::new(authn_pb::CreateUserRequest {
            username: format!("ryw_{suffix}"),
            email: format!("ryw_{suffix}@example.com"),
            password: "CorrectHorse1!".to_string(),
            tenant_id: "acme".to_string(),
            full_name: "RYW Live".to_string(),
            project_id: "billing".to_string(),
            ..Default::default()
        }))
        .await
        .expect("create_user")
        .into_inner();
    let user = created.user.expect("created user");

    // CreateUser → GetUser
    assert_create_then_get("CreateUser→GetUser", &user.user_id, |id| {
        let svc = &svc;
        async move {
            let got = svc
                .get_user(Request::new(authn_pb::GetUserRequest {
                    user_id: id.clone(),
                    ..Default::default()
                }))
                .await?
                .into_inner()
                .user;
            Ok(got.is_some_and(|u| u.user_id == id))
        }
    })
    .await;

    // CreateUser → ListUsers (same tenant metadata) finds the returned id.
    assert_create_then_get("CreateUser→ListUsers", &user.user_id, |id| {
        let svc = &svc;
        async move {
            let listed = svc
                .list_users(Request::new(authn_pb::ListUsersRequest {
                    tenant_id: "acme".to_string(),
                    ..Default::default()
                }))
                .await?
                .into_inner();
            Ok(listed.users.iter().any(|u| u.user_id == id))
        }
    })
    .await;

    cleanup_native_auth_db(&pool).await;
}
